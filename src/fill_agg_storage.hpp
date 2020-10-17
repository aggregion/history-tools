#pragma once
#include <appbase/application.hpp>

namespace agg_model {

struct fill_status {
    uint32_t    head;
    std::string head_id;
    uint32_t    irreversible;
    std::string irreversible_id;
};

struct block_info {
    uint32_t    block_num;
    std::string timestamp;
};

struct permission {
    uint32_t    block_num;
    std::string owner;
    std::string public_key;
};

struct transfer {
    uint32_t    block_num;
    std::string transaction_id;
    uint32_t    action_ordinal;
    uint64_t    receipt_recv_sequence;
    std::string receiver;
    std::string act_data;
};

} // namespace agg_model

struct agg_storage {

    agg_storage();

    void create_schema();
    bool is_schema_exists();
    void drop_schema();
    void truncate(uint32_t block_num);
    void load(agg_model::fill_status&);
    void write(const agg_model::fill_status&);
    void write(const agg_model::transfer&);
    void write(const agg_model::block_info&);
    void write(const agg_model::permission&);

    std::vector<uint32_t> fetch_blocks_without_timestamp();

  private:
    struct impl;
    std::shared_ptr<agg_storage::impl> my;
};
