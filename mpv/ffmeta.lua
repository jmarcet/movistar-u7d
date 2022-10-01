local msg = require("mp.msg")
local utils = require("mp.utils")

local function load_ffmeta()
  local filename = mp.get_property("stream-open-filename")
  local ext = filename:match("^.+(%..+)$")
  if ext then
    local chapters = filename:gsub(ext, ".ffmeta")
    if utils.file_info(chapters) then
      mp.set_property("file-local-options/chapters-file", chapters)
      msg.info("Loaded chapters file:", mp.get_property("file-local-options/chapters-file"))
    end
  end
end

mp.register_event("start-file", load_ffmeta)
