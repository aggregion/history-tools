#include "fill_agg_plugin.hpp"
#include "fill_agg_storage.hpp"
#include "state_history_connection.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/process.hpp>
#include <fc/exception/exception.hpp>

using namespace abieos;
using namespace appbase;
using namespace state_history;
using namespace std::literals;

namespace asio      = boost::asio;
namespace bpo       = boost::program_options;
namespace websocket = boost::beast::websocket;

using asio::ip::tcp;
using boost::beast::flat_buffer;
using boost::system::error_code;

struct agg_session;

struct fill_aggregion_config : connection_config {
    uint32_t truncate_after = 0;
    uint32_t skip_to        = 0;
    uint32_t stop_before    = 0;
    bool     reset_schema   = false;
};

struct fill_aggregion_plugin_impl : std::enable_shared_from_this<fill_aggregion_plugin_impl> {
    std::shared_ptr<fill_aggregion_config> config = std::make_shared<fill_aggregion_config>();
    std::shared_ptr<agg_session>           session;
    boost::asio::deadline_timer            timer;

    fill_aggregion_plugin_impl()
        : timer(app().get_io_service()) {}

    ~fill_aggregion_plugin_impl();

    void schedule_retry() {
        timer.expires_from_now(boost::posix_time::seconds(1));
        timer.async_wait([this](auto&) {
            ilog("retry...");
            start();
        });
    }

    void start();
};

struct aux_connection : connection_callbacks, std::enable_shared_from_this<aux_connection> {
    boost::asio::io_context&                   ioc;
    const fill_aggregion_config&               config;
    agg_storage&                               storage;
    std::shared_ptr<state_history::connection> connection;
    std::set<uint32_t>                         pending_requests;

    aux_connection(boost::asio::io_context& ioc, const fill_aggregion_config& config, agg_storage& storage)
        : ioc{ioc}
        , config{config}
        , storage{storage} {
        auto bwots = storage.fetch_blocks_without_timestamp();
        pending_requests.insert(bwots.begin(), bwots.end());
    }

    void request_block(uint32_t block_num) {
        pending_requests.emplace(block_num);
        if (connection)
            return;

        connection = std::make_shared<state_history::connection>(ioc, config, shared_from_this());
        connection->connect();
    }

    void process_pending_requests() {
        if (pending_requests.empty())
            return;

        auto block_num = *pending_requests.begin();

        get_blocks_request_v0 req;
        req.start_block_num        = block_num;
        req.end_block_num          = block_num + 1;
        req.max_messages_in_flight = 0xffff'ffff;
        req.irreversible_only      = false;
        req.fetch_block            = true;
        req.fetch_traces           = false;
        req.fetch_deltas           = false;
        connection->send(req);
    }

    void received_abi(std::string_view abi) override { process_pending_requests(); }

    bool received(get_blocks_result_v0& result) override {
        pending_requests.erase(result.this_block->block_num);
        if (result.block)
            receive_block(result.this_block->block_num, *result.block);
        process_pending_requests();
        return true;
    }

    void receive_block(uint32_t block_num, input_buffer bin) {
        signed_block block;
        bin_to_native(block, bin);
        agg_model::block_info bi;
        bi.block_num = block_num;
        bi.timestamp = static_cast<std::string>(block.timestamp);
        storage.write(bi);
    }

    void closed(bool) override { connection.reset(); }
};

struct agg_session : connection_callbacks, std::enable_shared_from_this<agg_session> {
    fill_aggregion_plugin_impl*            my = nullptr;
    std::shared_ptr<fill_aggregion_config> config;

    agg_storage                                storage;
    std::shared_ptr<state_history::connection> main_channel;
    std::shared_ptr<aux_connection>            aux_channel;

    agg_model::fill_status fill_status;

    agg_session(fill_aggregion_plugin_impl* my)
        : my(my)
        , config(my->config) {}

    void start(asio::io_context& ioc) {
        if (config->reset_schema) {
            storage.drop_schema();
            config->reset_schema = false;
        }

        if (!storage.is_schema_exists()) {
            storage.create_schema();
        }

        if (config->truncate_after > 0) {
            storage.truncate(config->truncate_after);
            config->truncate_after = 0;
        }

        main_channel = std::make_shared<state_history::connection>(ioc, *config, shared_from_this());
        main_channel->connect();

        aux_channel = std::make_shared<aux_connection>(ioc, *config, storage);
    }

    void received_abi(std::string_view abi) override { main_channel->send(get_status_request_v0{}); }

    bool received(get_status_result_v0&) override {
        storage.load(fill_status);
        storage.truncate(fill_status.head + 1);

        get_blocks_request_v0 req;
        req.start_block_num        = std::max(config->skip_to, fill_status.head + 1);
        req.end_block_num          = 0xffff'ffff;
        req.max_messages_in_flight = 0xffff'ffff;
        req.irreversible_only      = false;
        req.fetch_block            = true;
        req.fetch_traces           = true;
        req.fetch_deltas           = true;
        main_channel->send(req);
        return true;
    }

    bool received(get_blocks_result_v0& result) override {
        if (!result.this_block)
            return true;

        if (config->stop_before && result.this_block->block_num >= config->stop_before) {
            ilog("block ${b}: stop requested", ("b", result.this_block->block_num));
            return false;
        }

        if (result.this_block->block_num <= fill_status.head) {
            ilog("switch forks at block ${b}", ("b", result.this_block->block_num));
            storage.truncate(result.this_block->block_num);
        }

        if (result.deltas)
            receive_deltas(result.this_block->block_num, *result.deltas);
        if (result.traces)
            receive_traces(result.this_block->block_num, *result.traces);

        fill_status.head            = result.this_block->block_num;
        fill_status.head_id         = static_cast<std::string>(result.this_block->block_id);
        fill_status.irreversible    = result.last_irreversible.block_num;
        fill_status.irreversible_id = static_cast<std::string>(result.last_irreversible.block_id);

        storage.write(fill_status);

        return true;
    }

    void receive_deltas(uint32_t block_num, input_buffer bin) {
        auto num = read_varuint32(bin);
        for (uint32_t i = 0; i < num; ++i) {
            check_variant(bin, main_channel->get_type("table_delta"), "table_delta_v0");
            table_delta_v0 table_delta;
            bin_to_native(table_delta, bin);

            if (table_delta.name != "permission")
                continue;

            auto& variant_type = main_channel->get_type(table_delta.name);
            if (!variant_type.filled_variant || variant_type.fields.size() != 1 || !variant_type.fields[0].type->filled_struct)
                throw std::runtime_error("don't know how to proccess " + variant_type.name);

            for (auto& row : table_delta.rows) {
                check_variant(row.data, variant_type, 0u);

                agg_model::permission p;
                p.block_num = block_num;
                p.owner     = static_cast<std::string>(abieos::read_raw<abieos::name>(row.data));

                (void)abieos::read_raw<abieos::name>(row.data);       // skip 'name'
                (void)abieos::read_raw<abieos::name>(row.data);       // skip 'parent'
                (void)abieos::read_raw<abieos::time_point>(row.data); // skip 'last_updated'
                (void)abieos::read_raw<uint32_t>(row.data);           // skip 'auth threshold'

                auto keys_count = read_varuint32(row.data);
                for (uint32_t i = 0; i < keys_count; ++i) {
                    p.public_key = public_key_to_string(abieos::read_raw<abieos::public_key>(row.data));
                    (void)abieos::read_raw<uint16_t>(row.data); // skip 'key weight'
                    storage.write(p);
                }
            }
        }
    }

    void receive_traces(uint32_t block_num, input_buffer bin) {
        auto     num          = read_varuint32(bin);
        uint32_t num_ordinals = 0;

        for (uint32_t i = 0; i < num; ++i) {
            transaction_trace trx_trace;
            bin_to_native(trx_trace, bin);
            auto transaction = std::get<0>(trx_trace);

            bool has_transfers = false;
            for (auto& atracev : transaction.action_traces) {
                const action_trace_v0& action = std::get<action_trace_v0>(atracev);

                if (action.act.name == abieos::name{"onblock"} || action.act.name == abieos::name{"setabi"} ||
                    action.act.name == abieos::name{"setcode"}) {
                    continue;
                }

                const auto is_transfer = action.act.name == abieos::name{"transfer"} && action.act.account == abieos::name{"eosio.token"};
                if (!is_transfer)
                    continue;

                agg_model::transfer t;
                t.block_num             = block_num;
                t.transaction_id        = static_cast<std::string>(transaction.id);
                t.action_ordinal        = action.action_ordinal.value;
                t.receipt_recv_sequence = std::get<action_receipt_v0>(*action.receipt).recv_sequence;
                t.receiver              = static_cast<std::string>(action.receiver);
                abieos::hex(action.act.data.pos, action.act.data.end, back_inserter(t.act_data));

                storage.write(t);
                has_transfers = true;
            }
            if (has_transfers)
                aux_channel->request_block(block_num);
        }
    }

    void shutdown() { main_channel->close(false); }

    void closed(bool retry) override {
        if (my) {
            my->session.reset();
            if (retry)
                my->schedule_retry();
        }
    }
};

static abstract_plugin& _fill_aggregion_plugin = app().register_plugin<fill_agg_plugin>();

fill_aggregion_plugin_impl::~fill_aggregion_plugin_impl() {
    if (session)
        session->my = nullptr;
}

void fill_aggregion_plugin_impl::start() {
    session = std::make_shared<agg_session>(this);
    session->start(app().get_io_service());
}

fill_agg_plugin::fill_agg_plugin()
    : my(std::make_shared<fill_aggregion_plugin_impl>()) {}

fill_agg_plugin::~fill_agg_plugin() {}

void fill_agg_plugin::set_program_options(options_description& cli, options_description& cfg) {
    auto clop = cli.add_options();
    clop("fagg-reset", "Cleanup database schema ('agg')");
    clop("fagg-truncate-after", bpo::value<uint32_t>(), "Delete blocks after specified block num");
}

void fill_agg_plugin::plugin_initialize(const variables_map& options) {
    try {
        auto endpoint = options.at("fill-connect-to").as<std::string>();
        if (endpoint.find(':') == std::string::npos)
            throw std::runtime_error("invalid endpoint: " + endpoint);

        auto port                  = endpoint.substr(endpoint.find(':') + 1, endpoint.size());
        auto host                  = endpoint.substr(0, endpoint.find(':'));
        my->config->host           = host;
        my->config->port           = port;
        my->config->reset_schema   = options.count("fagg-reset");
        my->config->skip_to        = options.count("fill-skip-to") ? options["fill-skip-to"].as<uint32_t>() : 0;
        my->config->stop_before    = options.count("fill-stop") ? options["fill-stop"].as<uint32_t>() : 0;
        my->config->truncate_after = options.count("fagg-truncate-after") ? options["fagg-truncate-after"].as<uint32_t>() : 0;
    }
    FC_LOG_AND_RETHROW()
}

void fill_agg_plugin::plugin_startup() { my->start(); }

void fill_agg_plugin::plugin_shutdown() {
    if (my->session)
        my->session->shutdown();
    my->timer.cancel();
    ilog("fill_agg_plugin stopped");
}
