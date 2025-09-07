wrk.method = "PATCH"
wrk.body = '{"age": 31, "city": "New York"}'
wrk.headers["Content-Type"] = "application/json"

function response(status, headers, body)
   if status ~= 200 then
      print("Error: " .. status .. " - " .. body)
   end
end
