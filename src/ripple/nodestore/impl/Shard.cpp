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

#include <BeastConfig.h>
#include <ripple/nodestore/impl/Shard.h>
#include <ripple/app/ledger/InboundLedgers.h>
#include <ripple/nodestore/Manager.h>

namespace ripple {
namespace NodeStore {

Shard::Shard(ShardConfig cfg, std::uint32_t index, beast::Journal& j)
    : index_ {index}
    , firstSeq_ {std::max (detail::genesisSeq, cfg.firstSeq(index))}
    , lastSeq_ {cfg.lastSeq(index)}
    , pCache_ ("shard " + std::to_string(index), cacheTargetSize,
        cacheTargetSeconds, stopwatch(), j)
    , nCache_ ("shard " + std::to_string(index), stopwatch(),
        cacheTargetSize, cacheTargetSeconds)
    , j_(j)
    , shardConfig_ {cfg}
{
    assert(index_ >= shardConfig_.genesisShardIndex());
}

bool
Shard::open(Section config, Scheduler& scheduler,
    boost::filesystem::path dir)
{
    assert(!backend_);
    using namespace boost::filesystem;
    dir_ = dir / std::to_string(index_);
    config.set("path", dir_.string());
    auto newShard {!is_directory(dir_) || is_empty(dir_)};
    try
    {
        backend_ = Manager::instance().make_Backend(
            config, scheduler, j_);
    }
    catch (std::exception const& e)
    {
        JLOG(j_.error()) <<
            "shard " << std::to_string(index_) <<
            " exception: " << e.what();
        return false;
    }

    if (backend_->fdlimit() == 0)
        return true;

    control_ = dir_ / controlFileName;
    if (newShard)
    {
        if (!saveControl())
            return false;
    }
    else if (is_regular_file(control_))
    {
        std::ifstream ifs(control_.string());
        if (!ifs.is_open())
        {
            JLOG(j_.error()) <<
                "shard " << std::to_string(index_) <<
                " unable to open control file";
            return false;
        }
        boost::archive::text_iarchive ar(ifs);
        ar & storedSeqs_;
        if (!storedSeqs_.empty())
        {
            if (boost::icl::first(storedSeqs_) < firstSeq_ ||
                boost::icl::last(storedSeqs_) > lastSeq_)
            {
                JLOG(j_.error()) <<
                    "shard " << std::to_string(index_) <<
                    " invalid control file";
                return false;
            }
            if (boost::icl::length(storedSeqs_) ==
                (index_ == shardConfig_.genesisShardIndex() ?
                    shardConfig_.genesisNumLedgers() : shardConfig_.ledgersPerShard()))
            {
                JLOG(j_.error()) <<
                    "shard " << std::to_string(index_) <<
                    " found control file for complete shard";
                storedSeqs_.clear();
                remove(control_);
                complete_ = true;
            }
        }
    }
    else
        complete_ = true;
    updateFileSize();
    return true;
}

bool
Shard::setStored(std::shared_ptr<Ledger const> const& l)
{
    assert(backend_&& !complete_);
    if (boost::icl::contains(storedSeqs_, l->info().seq))
    {
        assert(false);
        JLOG(j_.error()) <<
            "shard " << std::to_string(index_) <<
            " ledger seq " << std::to_string(l->info().seq) <<
            " already stored";
        return false;
    }
    if (boost::icl::length(storedSeqs_) >=
            ((index_ == shardConfig_.genesisShardIndex() ?
                shardConfig_.genesisNumLedgers() :
                shardConfig_.ledgersPerShard())
            - 1)
       )
    {
        if (backend_->fdlimit() != 0)
        {
            remove(control_);
            updateFileSize();
        }
        complete_ = true;
        storedSeqs_.clear();

        JLOG(j_.debug()) <<
            "shard " << std::to_string(index_) << " complete";
    }
    else
    {
        if (backend_->fdlimit() != 0 && !saveControl())
            return false;
        storedSeqs_.insert(l->info().seq);
        lastStored_ = l;
    }

    JLOG(j_.debug()) <<
        "shard " << std::to_string(index_) <<
        " ledger seq " << std::to_string(l->info().seq) <<
        " stored";

    return true;
}

boost::optional<std::uint32_t>
Shard::prepare()
{
    if (storedSeqs_.empty())
         return lastSeq_;
    return prevMissing(storedSeqs_, 1 + lastSeq_, firstSeq_);
}

bool
Shard::hasLedger(std::uint32_t seq) const
{
    if (seq < firstSeq_ || seq > lastSeq_)
        return false;
    if (complete_)
        return true;
    return boost::icl::contains(storedSeqs_, seq);
}

void
Shard::validate(Application& app)
{
    uint256 hash;
    std::uint32_t seq;
    std::shared_ptr<Ledger> l;
    // Find the hash of the last ledger in this shard
    {
        std::tie(l, seq, hash) = loadLedgerHelper(
            "WHERE LedgerSeq >= " + std::to_string(lastSeq_) +
            " order by LedgerSeq asc limit 1", app);
        if (!l)
        {
            JLOG(j_.fatal()) <<
                "shard " << std::to_string(index_) <<
                " unable to validate. No lookup data";
            return;
        }
        if (seq != lastSeq_)
        {
            l->setImmutable(app.config());
            auto h = hashOfSeq(*l, lastSeq_, j_);
            if (!h)
            {
                JLOG(j_.fatal()) <<
                    "shard " << std::to_string(index_) <<
                    " unable to validate. No hash for ledger seq " <<
                    std::to_string(lastSeq_);
                return;
            }
            hash = *h;
            seq = lastSeq_;
        }
    }

    JLOG(j_.fatal()) <<
        "Validating shard " << std::to_string(index_) <<
        " ledgers " << std::to_string(firstSeq_) << "-" <<
        std::to_string(lastSeq_);

    std::shared_ptr<Ledger const> next;
    while (seq >= firstSeq_)
    {
        std::shared_ptr<NodeObject> nObj;
        try
        {
            if (backend_->fetch(hash.begin(), &nObj) == dataCorrupt)
            {
                JLOG(j_.fatal()) <<
                    "shard " << std::to_string(index_) <<
                    " corrupt NodeObject hash " << hash;
                break;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(j_.fatal()) <<
                "shard " << std::to_string(index_) <<
                " exception: " << e.what();
            break;
        }
        if (!nObj)
            break;
        l = std::make_shared<Ledger>(
            InboundLedger::deserializeHeader(makeSlice(nObj->getData()),
                true), app.config(), *app.shardFamily());
        if (l->info().hash != hash || l->info().seq != seq)
        {
            JLOG(j_.fatal()) <<
                "shard " << std::to_string(index_) <<
                " hash " << hash <<
                " seq " << std::to_string(seq) <<
                " cannot be a ledger";
            break;
        }
        l->stateMap().setLedgerSeq(seq);
        l->txMap().setLedgerSeq(seq);
        l->setImmutable(app.config());
        if (!l->stateMap().fetchRoot(
            SHAMapHash {l->info().accountHash}, nullptr))
        {
            JLOG(j_.fatal()) <<
                "shard " << std::to_string(index_) <<
                " ledger seq " << std::to_string(seq) <<
                " missing Account State root";
            break;
        }
        if (l->info().txHash.isNonZero())
        {
            if (!l->txMap().fetchRoot(
                SHAMapHash {l->info().txHash}, nullptr))
            {
                JLOG(j_.fatal()) <<
                    "shard " << std::to_string(index_) <<
                    " ledger seq " << std::to_string(seq) <<
                    " missing TX root";
                break;
            }
        }
        if (!valLedger(l, next))
            break;
        hash = l->info().parentHash;
        --seq;
        next = l;
    }
    if (seq < firstSeq_)
    {
        JLOG(j_.fatal()) <<
            "Valid and complete.";
    }
    else if (complete_)
    {
        JLOG(j_.fatal()) <<
            "Invalid, failed on seq " << std::to_string(seq) <<
            " hash " << to_string(hash);
    }
    else
    {
        JLOG(j_.fatal()) <<
            "Incomplete, stopped at seq " << std::to_string(seq) <<
            " hash " << to_string(hash);
    }
}

bool
Shard::valLedger(std::shared_ptr<Ledger const> const& l,
    std::shared_ptr<Ledger const> const& next)
{
    if (l->info().hash.isZero() || l->info().accountHash.isZero())
    {
        JLOG(j_.fatal()) <<
            "shard " << std::to_string(index_) <<
            " ledger seq " << std::to_string(l->info().seq) <<
            " invalid";
        return false;
    }
    bool error {false};
    auto f = [&, this](SHAMapAbstractNode& node) {
        auto hash {node.getNodeHash().as_uint256()};
        std::shared_ptr<NodeObject> nObj;
        try
        {
            if (backend_->fetch(hash.begin(), &nObj) == dataCorrupt)
            {
                JLOG(j_.fatal()) <<
                    "shard " << std::to_string(index_) <<
                    " ledger seq " << std::to_string(l->info().seq) <<
                    " corrupt NodeObject hash " << hash;
                error = true;
            }
        }
        catch (std::exception const& e)
        {
            JLOG(j_.fatal()) <<
                "shard " << std::to_string(index_) <<
                " ledger seq " << std::to_string(l->info().seq) <<
                " exception: " << e.what();
            error = true;
        }
        return !error;
    };

    if (l->stateMap().getHash().isNonZero())
    {
        if (!l->stateMap().isValid())
        {
            JLOG(j_.error()) <<
                "shard " << std::to_string(index_) <<
                " ledger seq " << std::to_string(l->info().seq) <<
                " state map invalid";
            return false;
        }
        try
        {
            if (next)
                l->stateMap().visitDifferences(&next->stateMap(), f);
            else
                l->stateMap().visitNodes(f);
        }
        catch (std::exception const& e)
        {
            JLOG(j_.fatal()) <<
                "shard " << std::to_string(index_) <<
                " ledger seq " << std::to_string(l->info().seq) <<
                " exception: " << e.what();
            return false;
        }
        if (error)
            return false;
    }
    if (l->info().txHash.isNonZero())
    {
        if (!l->txMap().isValid())
        {
            JLOG(j_.error()) <<
                "shard " << std::to_string(index_) <<
                " ledger seq " << std::to_string(l->info().seq) <<
                " transaction map invalid";
            return false;
        }
        try
        {
            l->txMap().visitNodes(f);
        }
        catch (std::exception const& e)
        {
            JLOG(j_.fatal()) <<
                "shard " << std::to_string(index_) <<
                " ledger seq " << std::to_string(l->info().seq) <<
                " exception: " << e.what();
            return false;
        }
        if (error)
            return false;
    }
    return true;
};

void
Shard::updateFileSize()
{
    fileSize_ = 0;
    using namespace boost::filesystem;
    for (auto const& de : directory_iterator(dir_))
        if (is_regular_file(de))
            fileSize_ += file_size(de);
}

bool
Shard::saveControl()
{
    std::ofstream ofs {control_.string(), std::ios::trunc};
    if (!ofs.is_open())
    {
        JLOG(j_.fatal()) <<
            "shard " << std::to_string(index_) <<
            " unable to save control file";
        return false;
    }
    boost::archive::text_oarchive ar(ofs);
    ar & storedSeqs_;
    return true;
}

} // NodeStore
} // ripple
