wrk.method = "POST"
wrk.body = '{"name": "wrk_user", "age": 30, "email": "wrk@example.com"}'
wrk.headers["Content-Type"] = "application/json"

function response(status, headers, body)
   if status ~= 201 then
      print("Error: " .. status .. " - " .. body)
   end
end
