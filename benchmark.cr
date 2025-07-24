require "benchmark"

require "./src/trove.cr"
require "./spec/common.cr"

opts = Sophia::H{"compression"      => "zstd",
                 "compaction.cache" => 2_i64 * 1024 * 1024 * 1024}
env = Trove::Env.new Sophia::H{"sophia.path" => "/tmp/trove"}, {d: opts, i: opts, u: opts}
chest = Trove::Chest.new env

cs = JSON.parse COMPLEX_STRUCTURE.to_json
k = "level1.level2.level3.1.metadata.level4.level5.level6.note"
v = "This is six levels deep"

Benchmark.ips do |b|
  b.report "set+delete" do
    chest.delete chest << cs
  end
end

i = chest << cs
env.checkpoint
Benchmark.ips do |b|
  b.report "has key" do
    raise "Can not get" unless chest.has_key? i, k
  end
  b.report "has key (only simple)" do
    raise "Can not get" unless chest.has_key! i, k
  end
  b.report "get full" do
    raise "Can not get" if chest.get(i) != cs
  end
  b.report "get field" do
    raise "Can not get" if chest.get(i, k) != v
  end
  b.report "get field (only simple)" do
    raise "Can not get" if chest.get!(i, k) != v
  end
end
Benchmark.ips do |b|
  n = 10**4 - 1
  (1..n).each { chest << cs }
  b.report "get one oid from index (unique)" do
    chest.unique k, v
  end
  b.report "get one oid from index" do
    chest.where!(k, v) { |ii| break }
  end
  b.report "get #{n + 1} oids from index" do
    g = 0
    chest.where!(k, v) { |ii| g += 1 }
    raise "#{g} != #{n + 1}" if g != n + 1
  end
end
