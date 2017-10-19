//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#ifndef RIPPLE_NODESTORE_SHARD_H_INCLUDED
#define RIPPLE_NODESTORE_SHARD_H_INCLUDED

#include <ripple/app/ledger/Ledger.h>
#include <ripple/basics/BasicConfig.h>
#include <ripple/basics/RangeSet.h>
#include <ripple/nodestore/DatabaseShard.h>
#include <ripple/nodestore/NodeObject.h>
#include <ripple/nodestore/Scheduler.h>

#include <boost/filesystem.hpp>
#include <boost/serialization/map.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

namespace ripple {
namespace NodeStore {

namespace detail {

/** this is a system constant/invariant: **/
constexpr static std::uint32_t genesisSeq {32570u};

}

class ShardConfig
{
    std::uint32_t ledgersPerShard_;
    std::uint32_t genesisShardIndex_;
    std::uint32_t genesisNumLedgers_;
    std::uint64_t avgShardSz_;

public:

    /** The number of ledgers stored in a shard
     *  default: 16k
     *  This value should only be changed for unit tests
     **/
    explicit ShardConfig(std::uint32_t lps = 16384u)
    {
        setLedgersPerShard(lps);
    }

    auto inline genesisShardIndex() const { return genesisShardIndex_; }
    auto inline genesisNumLedgers() const { return genesisNumLedgers_; }
    auto inline ledgersPerShard() const { return ledgersPerShard_; }
    auto inline averageShardSize() const { return avgShardSz_; }

    inline void setLedgersPerShard(std::uint32_t lps)
    {
        // this is currently only called during init, so
        // thread safety of the object state is not critical
        // (thus no locking needed here at this time)
        ledgersPerShard_ = lps;
        genesisShardIndex_ = seqToShardIndex(detail::genesisSeq);
        genesisNumLedgers_ =
            ledgersPerShard_ - (detail::genesisSeq - firstSeq(genesisShardIndex_));
        // Average disk space a shard requires (in bytes)
        avgShardSz_  = ledgersPerShard_ * (192 * 1024);
    }

    inline void setAverageShardSize(std::uint64_t avs)
    {
        avgShardSz_ = avs;
    }

    // Return the first ledger sequence of the shard index
    inline
    std::uint32_t
    firstSeq(std::uint32_t const shardIndex) const
    {
        return 1 + (shardIndex * ledgersPerShard_);
    }

    /** Finds a shard index containing the ledger sequence

        @param seq The ledger sequence
        @return The shard index
    */
    inline
    std::uint32_t
    seqToShardIndex(std::uint32_t const seq) const
    {
        return (seq - 1) / ledgersPerShard_;
    }

    // Return the last ledger sequence of the shard index
    inline
    std::uint32_t
    lastSeq(std::uint32_t const shardIndex) const
    {
        return (shardIndex + 1) * ledgersPerShard_;
    }

    inline
    auto genesisShardIndex()
    {
        return seqToShardIndex(detail::genesisSeq);
    }
};


/* A range of historical ledgers backed by a nodestore.
   Shards are indexed and store `ledgersPerShard`.
   Shard `i` stores ledgers starting with sequence: `1 + (i * ledgersPerShard)`
   and ending with sequence: `(i + 1) * ledgersPerShard`.
   Once a shard has all its ledgers, it is marked as read only
   and is never written to again.
*/
class Shard
{
public:
    explicit
    Shard(ShardConfig cfg, std::uint32_t index, beast::Journal& j);

    bool
    open(Section config, Scheduler& scheduler,
        boost::filesystem::path dir);

    bool
    setStored(std::shared_ptr<Ledger const> const& l);

    boost::optional<std::uint32_t>
    prepare();

    bool
    hasLedger(std::uint32_t seq) const;

    void
    validate(Application& app);

    std::uint32_t
    index() const { return index_; }

    bool
    complete() const { return complete_; }

    TaggedCache<uint256, NodeObject>&
    pCache() { return pCache_; }

    KeyCache<uint256>&
    nCache() { return nCache_; }

    std::uint64_t
    fileSize() const { return fileSize_; }

    Backend&
    getBackend() const
    {
        assert(backend_);
        return *backend_;
    }

    std::uint32_t
    fdlimit() const
    {
        assert(backend_);
        return backend_->fdlimit();
    }

    std::shared_ptr<Ledger const>
    lastStored() { return lastStored_; }

    std::uint32_t
    numComplete() const { return boost::icl::length(storedSeqs_); }

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        ar & storedSeqs_;
    }

    static constexpr auto controlFileName = "control.txt";

    // Shard Index
    std::uint32_t const index_;

    // First ledger sequence in this shard
    std::uint32_t const firstSeq_;

    // Last ledger sequence in this shard
    std::uint32_t const lastSeq_;

    // Database positive cache
    TaggedCache<uint256, NodeObject> pCache_;

    // Database negative cache
    KeyCache<uint256> nCache_;

    std::uint64_t fileSize_ {0};
    std::unique_ptr<Backend> backend_;
    beast::Journal j_;

    // Path to database files
    boost::filesystem::path dir_;

    // True if shard has its entire ledger range stored
    bool complete_{false};

    // Sequences of ledgers stored with an incomplete shard
    RangeSet<std::uint32_t> storedSeqs_;

    // Path to control file
    boost::filesystem::path control_;

    // Used as an optimization for visitDifferences
    std::shared_ptr<Ledger const> lastStored_;

    ShardConfig shardConfig_;
    bool
    valLedger(std::shared_ptr<Ledger const> const& l,
        std::shared_ptr<Ledger const> const& next);

    void
    updateFileSize();

    bool
    saveControl();
};

} // NodeStore
} // ripple

#endif
