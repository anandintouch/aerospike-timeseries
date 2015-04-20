-- Get a particular bin
function readBin(r,name)
    return r[name]
end


function searchListBin(stream,passenger,destination)

 info("current passenger value:"..tostring(passenger).."destination value="..tostring(destination))

 local function filter_passenger(record)
 	
   return record.Pax == passenger and record.dest == destination
  --  return  record.dest == destination
 end
 
 
 return stream : filter(filter_passenger) : map(map_paxdata)
end