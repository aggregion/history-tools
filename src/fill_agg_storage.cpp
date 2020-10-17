#include "fill_agg_storage.hpp"
#include <fc/log/logger.hpp>
#include <pqxx/pqxx>

using agg_model::block_info;
using agg_model::fill_status;
using agg_model::permission;
using agg_model::transfer;

struct scripts {
    constexpr static const auto drop_schema = R"(
            DROP SCHEMA IF EXISTS agg CASCADE;
        )";

    constexpr static const auto select_schema = R"(
        SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'agg';
    )";

    constexpr static const auto create_schema = R"(
            CREATE SCHEMA agg;

            CREATE TABLE agg.fill_status(
                head BIGINT,
                head_id VARCHAR(64),
                irreversible BIGINT,
                irreversible_id VARCHAR(64)
            );

            INSERT INTO agg.fill_status VALUES(0, '', 0, '');

            CREATE TABLE agg.transfer (
                block_num BIGINT,
                block_timestamp TIMESTAMP,
                transaction_id VARCHAR(64),
                action_ordinal BIGINT,
                receipt_recv_sequence BIGINT,
                receiver VARCHAR(13),
                act_data VARCHAR(256)
            );
            ALTER TABLE agg.transfer ADD CONSTRAINT agg_pk_transfer PRIMARY KEY (block_num, transaction_id, action_ordinal);
            CREATE INDEX agg_transfer_receiver_idx ON agg.transfer (receiver);

            CREATE TABLE agg.permission (
                block_num BIGINT,
                owner VARCHAR(13),
                public_key VARCHAR
            );
            ALTER TABLE agg.permission ADD CONSTRAINT agg_pk_permission PRIMARY KEY (owner, public_key);
            CREATE INDEX agg_permission_publickey_idx ON agg.permission (public_key);
        )";

    constexpr static const auto load_fill_status = R"(
            SELECT head, head_id, irreversible, irreversible_id FROM agg.fill_status;
        )";

    constexpr static const auto select_blocks_without_timestamp = R"(
            SELECT block_num FROM agg.transfer WHERE block_timestamp IS NULL;
        )";

    constexpr static const auto write_fill_status = R"(
            UPDATE agg.fill_status SET head = $1, head_id = $2, irreversible = $3, irreversible_id = $4;
        )";

    constexpr static const auto reset_fill_status = R"(
            UPDATE agg.fill_status SET head = $1, head_id = '', irreversible = 0, irreversible_id = '';
        )";

    constexpr static const auto insert_transfer = R"(
            INSERT INTO agg.transfer (
                block_num,
                block_timestamp,
                transaction_id,
                action_ordinal,
                receipt_recv_sequence,
                receiver,
                act_data)
            VALUES($1, NULL, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING;
        )";

    constexpr static const auto insert_permission = R"(
            INSERT INTO agg.permission (
                block_num,
                owner,
                public_key)
            VALUES($1, $2, $3)
            ON CONFLICT DO NOTHING;
        )";

    constexpr static const auto truncate_transfers_after = R"(
            DELETE FROM agg.transfer WHERE block_num > $1;
        )";

    constexpr static const auto truncate_permissions_after = R"(
            DELETE FROM agg.permission WHERE block_num > $1;
        )";

    constexpr static const auto update_transfer_timestamp = R"(
            UPDATE agg.transfer SET block_timestamp = $2 WHERE block_num = $1;
        )";
};

struct agg_storage::impl {
    pqxx::connection db;

    impl() {
        db.prepare("write_fill_status", scripts::write_fill_status);
        db.prepare("reset_fill_status", scripts::reset_fill_status);
        db.prepare("insert_transfer", scripts::insert_transfer);
        db.prepare("insert_permission", scripts::insert_permission);
        db.prepare("update_transfer_timestamp", scripts::update_transfer_timestamp);
        db.prepare("truncate_transfers_after", scripts::truncate_transfers_after);
        db.prepare("truncate_permissions_after", scripts::truncate_permissions_after);
    }
};

agg_storage::agg_storage()
    : my{std::make_shared<agg_storage::impl>()} {}

void agg_storage::create_schema() {
    ilog("create schema agg");
    pqxx::work w(my->db);
    w.exec(scripts::create_schema);
    w.commit();
}

bool agg_storage::is_schema_exists() {
    pqxx::work w(my->db);

    auto rows = w.exec(scripts::select_schema);
    return rows.size() == 1;
}

void agg_storage::drop_schema() {
    ilog("drop schema agg");
    pqxx::work w(my->db);
    w.exec(scripts::drop_schema);
    w.commit();
}

void agg_storage::truncate(uint32_t block_num) {
    ilog("truncate history from ${b} block", ("b", block_num));
    pqxx::work w(my->db);
    w.exec_prepared0("truncate_transfers_after", block_num);
    w.exec_prepared0("truncate_permissions_after", block_num);
    w.exec_prepared0("reset_fill_status", block_num);
    w.commit();
}

void agg_storage::load(fill_status& fs) {
    pqxx::work w(my->db);

    auto r = w.exec(scripts::load_fill_status)[0];

    fs.head            = r[0].as<uint32_t>();
    fs.head_id         = r[1].as<std::string>();
    fs.irreversible    = r[2].as<uint32_t>();
    fs.irreversible_id = r[3].as<std::string>();
}

void agg_storage::write(const fill_status& fs) {
    pqxx::work w(my->db);
    w.exec_prepared0("write_fill_status", fs.head, fs.head_id, fs.irreversible, fs.irreversible_id);
    w.commit();
}

void agg_storage::write(const transfer& t) {
    if (t.act_data.size() > 256) {
        throw std::runtime_error("action data is too large");
    }
    pqxx::work w(my->db);
    w.exec_prepared0("insert_transfer", t.block_num, t.transaction_id, t.action_ordinal, t.receipt_recv_sequence, t.receiver, t.act_data);
    w.commit();
}

void agg_storage::write(const block_info& b) {
    pqxx::work w(my->db);
    w.exec_prepared0("update_transfer_timestamp", b.block_num, b.timestamp);
    w.commit();
}

void agg_storage::write(const permission& p) {
    pqxx::work w(my->db);
    w.exec_prepared0("insert_permission", p.block_num, p.owner, p.public_key);
    w.commit();
}

std::vector<uint32_t> agg_storage::fetch_blocks_without_timestamp() {
    pqxx::work w(my->db);

    auto rows = w.exec(scripts::select_blocks_without_timestamp);

    std::vector<uint32_t> result;
    result.reserve(rows.size());
    for (const auto& r : rows) {
        result.emplace_back(r[0].as<uint32_t>());
    }
    return result;
}
