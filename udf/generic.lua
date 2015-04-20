-- $RCSfile: dsc-query.lua,v $
--
-- $Revision: 1.1.2.2 $
--
-- $Id: dsc-query.lua,v 1.1.2.2 2014/09/08 19:33:40 huberth Exp $
--
-- Copyright (c) 2014 Alcatel-Lucent. All rights reserved.
-- Please read the associated COPYRIGHTS file for more details.
--

-- Perform a generic query based on the expression tokens.
--
-- in:
--   aInStream - the stream to filter
--   aInBins   - the list of bin names to return
--   ...       - the filter expression tokens
--
-- return:
--   the filtering stream
function genericQuery(aInStream, aInBins, ...)
    -- Determine whether the record matches the filter expression.
    --
    -- in:
    --   aInRecord - the record to filter
    --
    -- return:
    --   true if the record matches the filter, false otherwise
    local function filterValues(aInRecord)
	return true
    end

    -- Convert the record to a map.
    --
    -- in:
    --   aInRecord - the record to convert
    --
    -- return:
    --   a map containing the record bin values, plus an entry ['$generation']
    --   for the record generation
    local function mapRecord(aInRecord)
        local lMap = map()
        lMap['$generation'] = record.gen(aInRecord)
        lMap['$expiration'] = record.ttl(aInRecord)
        local i = 1
        while aInBins[i] do
            local lBin = aInBins[i]
            lMap[lBin] = aInRecord[lBin]
            i = i + 1
        end
        return lMap
    end

    return aInStream : filter(filterValues) : map(mapRecord)
end

-- Perform a generic count based on the expression tokens.
--
-- in:
--   aInStream - the stream to filter
--   ...       - the filter expression tokens
--
-- return:
--   the filtering/mapping/aggregating stream
function genericCount(aInStream, ...)
    local function filterValues(aInRecord)
        return true
    end

    local function one(aInRecord)
        return 1
    end

    local function add(aInLeft, aInRight)
        return aInLeft + aInRight
    end

    return aInStream : filter(filterValues) : map(one) : reduce(add)
end
