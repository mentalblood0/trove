require "benchmark"

require "./src/trove.cr"
require "./spec/common.cr"

opts = Sophia::H{"compression"      => "zstd",
                 "compaction.cache" => 2_i64 * 1024 * 1024 * 1024}
chest = Trove::Chest.new Trove::Env.new Sophia::H{"sophia.path" => "/tmp/trove"}, {d: Sophia::H.new, i: opts}

cs = JSON.parse COMPLEX_STRUCTURE.to_json

Benchmark.ips do |b|
  b.report "set+delete" do
    chest.delete chest << cs
  end
end
Benchmark.ips do |b|
  i = chest << cs
  b.report "get full" do
    raise "Can not get" if chest.get(i) != cs
  end
  b.report "get field" do
    raise "Can not get" if chest.get(i, "level1.level2.level3.1.metadata.level4.level5.level6.note") != "This is six levels deep"
  end
  b.report "get field (only simple)" do
    raise "Can not get" if chest.get!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note") != "This is six levels deep"
  end
  b.report "get oid from index (only first)" do
    chest.where! "level1.level2.level3.1.metadata.level4.level5.level6.note", "This is six levels deep"
  end
end
Benchmark.ips do |b|
  n = 10**4 - 1
  (1..n).each { chest << cs }
  b.report "get one oid from index" do
    chest.where("level1.level2.level3.1.metadata.level4.level5.level6.note", "This is six levels deep") { |ii| break }
  end
  b.report "get #{n} oids from index" do
    chest.where("level1.level2.level3.1.metadata.level4.level5.level6.note", "This is six levels deep") { |ii| next }
  end
end
