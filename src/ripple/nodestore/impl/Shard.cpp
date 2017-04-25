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
#include <ripple/nodestore/Manager.h>

#include <ripple/app/ledger/InboundLedgers.h>

namespace ripple {
namespace NodeStore {

Shard::Shard(std::uint32_t shardIndex)
    : index_ {shardIndex}
    , firstSeq_ {std::max(genesisSeq, detail::firstSeq(shardIndex))}
    , lastSeq_ {detail::lastSeq(shardIndex)}
{
    assert(index_ >= detail::genesisShardIndex);
}

bool
Shard::open(Section config, Scheduler& scheduler,
    boost::filesystem::path dir, beast::Journal& j)
{
    assert(!backend_);
    using namespace boost::filesystem;
    dir_ = dir / std::to_string(index_);
    config.set("path", dir_.string());
    auto newShard {!is_directory(dir_) || is_empty(dir_)};
    try
    {
        backend_ = Manager::instance().make_Backend(config, scheduler, j);
    }
    catch (std::exception const& e)
    {
        JLOG(j.error()) <<
            "Shard: Exception, " << e.what();
        return false;
    }

    if (backend_->fdlimit() == 0)
        return true;

    control_ = dir_ / controlFileName;
    if (newShard)
    {
        if (!saveControl(j))
            return false;
    }
    else if (is_regular_file(control_))
    {
        std::ifstream ifs(control_.string());
        if (!ifs.is_open())
        {
            JLOG(j.error()) <<
                "Unable to open control file";
            return false;
        }
        boost::archive::text_iarchive ar(ifs);
        ar & storedSeqs_;
        if (!storedSeqs_.empty())
        {
            if (boost::icl::first(storedSeqs_) < firstSeq_ ||
                boost::icl::last(storedSeqs_) > lastSeq_)
            {
                JLOG(j.error()) <<
                    "Invalid control file";
                return false;
            }
            if (boost::icl::length(storedSeqs_) ==
                (index_ == detail::genesisShardIndex ?
                    detail::genesisNumLedgers : ledgersPerShard))
            {
                JLOG(j.debug()) <<
                    "Found control file for complete shard";
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
Shard::setStored(std::shared_ptr<Ledger const> const& l,
    beast::Journal& j)
{
    assert(backend_&& !complete_);
    if (boost::icl::contains(storedSeqs_, l->info().seq))
    {
        assert(false);
        JLOG(j.error()) <<
            "ledger seq " <<
            std::to_string(l->info().seq) <<
            " already stored in shard " <<
            std::to_string(index_);
        return false;
    }
    if (boost::icl::length(storedSeqs_) >=
        (index_ == detail::genesisShardIndex ?
            detail::genesisNumLedgers : ledgersPerShard) - 1)
    {
        if (backend_->fdlimit() != 0)
        {
            remove(control_);
            updateFileSize();
        }
        complete_ = true;
        storedSeqs_.clear();

        JLOG(j.debug()) <<
            "shard " << std::to_string(index_) << " complete";
    }
    else
    {
        if (backend_->fdlimit() != 0 && !saveControl(j))
            return false;
        storedSeqs_.insert(l->info().seq);
        lastStored_ = l;
    }

    JLOG(j.debug()) <<
        "ledger seq " <<
        std::to_string(l->info().seq) <<
        " stored in shard " <<
        std::to_string(index_);

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
Shard::updateFileSize()
{
    fileSize_ = 0;
    using namespace boost::filesystem;
    for (auto const& de : directory_iterator(dir_))
        if (is_regular_file(de))
            fileSize_ += file_size(de);
}

bool
Shard::saveControl(beast::Journal& j)
{
    std::ofstream ofs {control_.string(), std::ios::trunc};
    if (!ofs.is_open())
    {
        JLOG(j.fatal()) <<
            "Unable to save control file";
        return false;
    }
    boost::archive::text_oarchive ar(ofs);
    ar & storedSeqs_;
    return true;
}

} // NodeStore
} // ripple
