require "benchmark"

require "./src/trove.cr"
require "./spec/common.cr"

opts = Sophia::H{"compression"      => "zstd",
                 "compaction.cache" => 2_i64 * 1024 * 1024 * 1024}
chest = Trove::Chest.new Trove::Env.new Sophia::H{"sophia.path" => "/tmp/trove"}, {d: Sophia::H.new, i: opts}

Benchmark.ips do |b|
  cs = JSON.parse COMPLEX_STRUCTURE.to_json
  b.report "set+delete" do
    chest.delete chest << cs
  end
  b.report "set" do
    chest << cs
  end
  i = chest << cs
  b.report "get full" do
    raise "Can not get" if chest[i]? != cs
  end
  b.report "get field" do
    raise "Can not get" if chest[i, "level1.level2.level3.1.metadata.level4.level5.level6.note"]? != "This is six levels deep"
  end
  b.report "get oid from index" do
    chest.where("level1.level2.level3.1.metadata.level4.level5.level6.note", "This is six levels deep") { |ii| break }
  end
end
