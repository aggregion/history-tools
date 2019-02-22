// copyright defined in LICENSE.txt

#pragma once
#include "lib-placeholders.hpp"
#include <eosiolib/time.hpp>

extern "C" {
   __attribute__((eosio_wasm_import))
   void exec_query(void* req_begin, void* req_end, void* cb_alloc_data, void* (*cb_alloc)(void* cb_alloc_data, size_t size));
}

template <typename T, typename Alloc_fn>
inline void exec_query(const T& req, Alloc_fn alloc_fn) {
    auto req_data = eosio::pack(req);
    exec_query(req_data.data(), req_data.data() + req_data.size(), &alloc_fn, [](void* cb_alloc_data, size_t size) -> void* {
        return (*reinterpret_cast<Alloc_fn*>(cb_alloc_data))(size);
    });
}

template <typename T>
inline std::vector<char> exec_query(const T& req) {
    std::vector<char> result;
    exec_query(req, [&result](size_t size) {
        result.resize(size);
        return result.data();
    });
    return result;
}

template <typename result, typename F>
bool for_each_query_result(const std::vector<char>& bytes, F f) {
    eosio::datastream<const char*> ds(bytes.data(), bytes.size());
    unsigned_int                   size;
    ds >> size;
    for (uint32_t i = 0; i < size.value; ++i) {
        eosio::datastream<const char*> row{nullptr, 0};
        ds >> row;
        result r;
        row >> r;
        if (!f(r))
            return false;
    }
    return true;
}

struct context_data {
    uint32_t           head            = {};
    eosio::checksum256 head_id         = {};
    uint32_t           irreversible    = {};
    eosio::checksum256 irreversible_id = {};
    uint32_t           first           = {};
};

template <typename F>
void for_each_member(context_data*, F f) {
    f("head", member_ptr<&context_data::head>{});
    f("head_id", member_ptr<&context_data::head_id>{});
    f("irreversible", member_ptr<&context_data::irreversible>{});
    f("irreversible_id", member_ptr<&context_data::irreversible_id>{});
    f("first", member_ptr<&context_data::first>{});
}

// todo: version number argument
extern "C" {
   __attribute__((eosio_wasm_import))
   void get_context_data(void* cb_alloc_data, void* (*cb_alloc)(void* cb_alloc_data, size_t size));
}

template <typename Alloc_fn>
inline void get_context_data(Alloc_fn alloc_fn) {
    get_context_data(&alloc_fn, [](void* cb_alloc_data, size_t size) -> void* { //
        return (*reinterpret_cast<Alloc_fn*>(cb_alloc_data))(size);
    });
}

inline context_data get_context_data() {
    context_data      result;
    std::vector<char> bin;
    get_context_data([&bin](size_t size) {
        bin.resize(size);
        return bin.data();
    });
    eosio::datastream<const char*> ds(bin.data(), bin.size());
    ds >> result;
    return result;
}

// todo: split out definitions useful for client-side
using absolute_block     = tagged_type<"absolute"_n, int32_t>;
using head_block         = tagged_type<"head"_n, int32_t>;
using irreversible_block = tagged_type<"irreversible"_n, int32_t>;
using block_select       = tagged_variant<serialize_tag_as_index, absolute_block, head_block, irreversible_block>;

// todo: which namespace?
inline std::string_view schema_type_name(block_select*) { return "eosio::block_select"; }

inline block_select make_absolute_block(int32_t i) {
    block_select result;
    result.value.emplace<0>(i);
    return result;
}

inline uint32_t get_block_num(const block_select& sel, const context_data& context) {
    switch (sel.value.index()) {
    case 0: return std::max((int32_t)0, std::get<0>(sel.value));
    case 1: return std::max((int32_t)0, (int32_t)context.head + std::get<1>(sel.value));
    case 2: return std::max((int32_t)0, (int32_t)context.irreversible + std::get<2>(sel.value));
    default: return 0x7fff'ffff;
    }
}

inline uint32_t increment(block_select& sel) {
    eosio_assert(sel.value.index() == 0, "can only increment absolute block_select");
    int32_t& result = std::get<0>(sel.value);
    result          = uint32_t(result) + 1;
    if (result < 0)
        result = 0;
    return result;
}

struct block_info {
    uint32_t                           block_num             = {};
    serial_wrapper<eosio::checksum256> block_id              = {};
    eosio::block_timestamp             timestamp             = eosio::block_timestamp{};
    eosio::name                        producer              = {};
    uint16_t                           confirmed             = {};
    serial_wrapper<eosio::checksum256> previous              = {};
    serial_wrapper<eosio::checksum256> transaction_mroot     = {};
    serial_wrapper<eosio::checksum256> action_mroot          = {};
    uint32_t                           schedule_version      = {};
    uint32_t                           new_producers_version = {};
    // std::vector<producer_key>       new_producers         = {}; // todo
};

inline std::string_view schema_type_name(block_info*) { return "block_info"; }

template <typename F>
void for_each_member(block_info*, F f) {
    f("block_num", member_ptr<&block_info::block_num>{});
    f("block_id", member_ptr<&block_info::block_id>{});
    f("timestamp", member_ptr<&block_info::timestamp>{});
    f("producer", member_ptr<&block_info::producer>{});
    f("confirmed", member_ptr<&block_info::confirmed>{});
    f("previous", member_ptr<&block_info::previous>{});
    f("transaction_mroot", member_ptr<&block_info::transaction_mroot>{});
    f("action_mroot", member_ptr<&block_info::action_mroot>{});
    f("schedule_version", member_ptr<&block_info::schedule_version>{});
    f("new_producers_version", member_ptr<&block_info::new_producers_version>{});
}

struct query_block_info_range_index {
    eosio::name query_name  = "block.info"_n;
    uint32_t    first       = {};
    uint32_t    last        = {};
    uint32_t    max_results = {};
};

enum class transaction_status : uint8_t {
    executed  = 0, // succeed, no error handler executed
    soft_fail = 1, // objectively failed (not executed), error handler executed
    hard_fail = 2, // objectively failed and error handler objectively failed thus no state change
    delayed   = 3, // transaction delayed/deferred/scheduled for future execution
    expired   = 4, // transaction expired and storage space refunded to user
};

struct action_trace {
    uint32_t                           block_index             = {};
    serial_wrapper<eosio::checksum256> transaction_id          = {};
    uint32_t                           action_index            = {};
    uint32_t                           parent_action_index     = {};
    ::transaction_status               transaction_status      = {};
    eosio::name                        receipt_receiver        = {};
    serial_wrapper<eosio::checksum256> receipt_act_digest      = {};
    uint64_t                           receipt_global_sequence = {};
    uint64_t                           receipt_recv_sequence   = {};
    unsigned_int                       receipt_code_sequence   = {};
    unsigned_int                       receipt_abi_sequence    = {};
    eosio::name                        account                 = {};
    eosio::name                        name                    = {};
    eosio::datastream<const char*>     data                    = {nullptr, 0};
    bool                               context_free            = {};
    int64_t                            elapsed                 = {};

    EOSLIB_SERIALIZE(
        action_trace, (block_index)(transaction_id)(action_index)(parent_action_index)(transaction_status)(receipt_receiver)(
                          receipt_act_digest)(receipt_global_sequence)(receipt_recv_sequence)(receipt_code_sequence)(receipt_abi_sequence)(
                          account)(name)(data)(context_free)(elapsed))
};

inline std::string_view schema_type_name(action_trace*) { return "action_trace"; }

struct query_action_trace_executed_range_name_receiver_account_block_trans_action {
    struct key {
        eosio::name                        name             = {};
        eosio::name                        receipt_receiver = {};
        eosio::name                        account          = {};
        uint32_t                           block_index      = {};
        serial_wrapper<eosio::checksum256> transaction_id   = {};
        uint32_t                           action_index     = {};
    };

    eosio::name query_name  = "at.e.nra"_n;
    uint32_t    max_block   = {};
    key         first       = {};
    key         last        = {};
    uint32_t    max_results = {};
};

struct account {
    uint32_t                           block_index      = {};
    bool                               present          = {};
    eosio::name                        name             = {};
    uint8_t                            vm_type          = {};
    uint8_t                            vm_version       = {};
    bool                               privileged       = {};
    eosio::time_point                  last_code_update = eosio::time_point{};
    serial_wrapper<eosio::checksum256> code_version     = {};
    eosio::block_timestamp_type        creation_date    = eosio::block_timestamp_type{};
    eosio::datastream<const char*>     code             = {nullptr, 0};
    eosio::datastream<const char*>     abi              = {nullptr, 0};

    EOSLIB_SERIALIZE(
        account, (block_index)(present)(name)(vm_type)(vm_version)(privileged)(last_code_update)(code_version)(creation_date)(code)(abi))
};

inline std::string_view schema_type_name(account*) { return "account"; }

template <typename F>
void for_each_member(account*, F f) {
    f("block_index", member_ptr<&account::block_index>{});
    f("present", member_ptr<&account::present>{});
    f("name", member_ptr<&account::name>{});
    f("vm_type", member_ptr<&account::vm_type>{});
    f("vm_version", member_ptr<&account::vm_version>{});
    f("privileged", member_ptr<&account::privileged>{});
    f("last_code_update", member_ptr<&account::last_code_update>{});
    f("code_version", member_ptr<&account::code_version>{});
    f("creation_date", member_ptr<&account::creation_date>{});
    f("code", member_ptr<&account::code>{});
    f("abi", member_ptr<&account::abi>{});
}

struct query_account_range_name {
    eosio::name query_name  = "account"_n;
    uint32_t    max_block   = {};
    eosio::name first       = {};
    eosio::name last        = {};
    uint32_t    max_results = {};
};

struct contract_row {
    uint32_t                       block_index = {};
    bool                           present     = {};
    eosio::name                    code        = {};
    uint64_t                       scope       = {};
    eosio::name                    table       = {};
    uint64_t                       primary_key = {};
    eosio::name                    payer       = {};
    eosio::datastream<const char*> value       = {nullptr, 0};
};

inline std::string_view schema_type_name(contract_row*) { return "contract_row"; }

template <typename payload, typename F>
bool for_each_contract_row(const std::vector<char>& bytes, F f) {
    return for_each_query_result<contract_row>(bytes, [&](contract_row& row) {
        payload p;
        if (row.present && row.value.remaining()) {
            // todo: don't assert if serialization fails
            row.value >> p;
            if (!f(row, &p))
                return false;
        } else {
            if (!f(row, nullptr))
                return false;
        }
        return true;
    });
}

struct query_contract_row_range_code_table_pk_scope {
    struct key {
        eosio::name code        = {};
        eosio::name table       = {};
        uint64_t    primary_key = {};
        uint64_t    scope       = {};
    };

    eosio::name query_name  = "cr.ctps"_n;
    uint32_t    max_block   = {};
    key         first       = {};
    key         last        = {};
    uint32_t    max_results = {};
};

struct query_contract_row_range_code_table_scope_pk {
    struct key {
        eosio::name code        = {};
        eosio::name table       = {};
        uint64_t    scope       = {};
        uint64_t    primary_key = {};
    };

    eosio::name query_name  = "cr.ctsp"_n;
    uint32_t    max_block   = {};
    key         first       = {};
    key         last        = {};
    uint32_t    max_results = {};
};

struct query_contract_row_range_scope_table_pk_code {
    struct key {
        uint64_t    scope       = {};
        eosio::name table       = {};
        uint64_t    primary_key = {};
        eosio::name code        = {};
    };

    eosio::name query_name  = "cr.stpc"_n;
    uint32_t    max_block   = {};
    key         first       = {};
    key         last        = {};
    uint32_t    max_results = {};
};

template <typename T>
struct contract_secondary_index_with_row {
    uint32_t                       block_index     = {};
    bool                           present         = {};
    eosio::name                    code            = {};
    uint64_t                       scope           = {};
    eosio::name                    table           = {};
    uint64_t                       primary_key     = {};
    eosio::name                    payer           = {};
    T                              secondary_key   = {};
    uint32_t                       row_block_index = {};
    bool                           row_present     = {};
    eosio::name                    row_payer       = {};
    eosio::datastream<const char*> row_value       = {nullptr, 0};
};

// todo: for_each_...

struct query_contract_index64_range_code_table_scope_sk_pk {
    struct key {
        eosio::name code          = {};
        eosio::name table         = {};
        uint64_t    scope         = {};
        uint64_t    secondary_key = {};
        uint64_t    primary_key   = {};
    };

    eosio::name query_name  = "ci1.cts2p"_n;
    uint32_t    max_block   = {};
    key         first       = {};
    key         last        = {};
    uint32_t    max_results = {};
};
