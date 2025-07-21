require "spec"

require "../src/trove"
require "./common.cr"

describe Trove do
  opts = Sophia::H{"compression"      => "zstd",
                   "compaction.cache" => 2_i64 * 1024 * 1024 * 1024}
  chest = Trove::Chest.new Trove::Env.new Sophia::H{"sophia.path" => "/tmp/trove"}, {d: Sophia::H.new, i: opts}

  [
    "string",
    1234,
    1234.1234,
    -1234,
    -1234.1234,
    0,
    0.0,
    true,
    false,
    nil,
    {"key" => "value"},
    {"a" => {"b" => "c"}},
    {"a" => {"b" => {"c" => "d"}}},
    {"a" => {"b" => {"c" => "d"}}},
    ["a", "b", "c"],
    COMPLEX_STRUCTURE,
  ].each do |o|
    it "add #{o}" do
      j = o.to_json
      i = chest << JSON.parse j
      r = chest[i]?
      r.should eq o
    end
  end
end
