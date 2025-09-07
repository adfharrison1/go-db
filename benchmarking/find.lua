wrk.method = "GET"

function response(status, headers, body)
   if status ~= 200 then
      print("Error: " .. status .. " - " .. body)
   end
end
