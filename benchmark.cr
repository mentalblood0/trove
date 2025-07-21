require "benchmark"

require "./src/trove.cr"
require "./spec/common.cr"

opts = Sophia::H{"compression"      => "zstd",
                 "compaction.cache" => 2_i64 * 1024 * 1024 * 1024}
chest = Trove::Chest.new Trove::Env.new Sophia::H{"sophia.path" => "/tmp/trove"}, {d: Sophia::H.new, i: opts}

Benchmark.ips do |b|
  cs = JSON.parse COMPLEX_STRUCTURE.to_json
  b.report "set" do
    chest << cs
  end
  i = (chest << cs)
  raise "Can not get" if chest[i]? != cs
  b.report "get" do
    chest[i]?
  end
end
