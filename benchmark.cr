require "benchmark"

require "./src/trove.cr"
require "./spec/common.cr"

alias Config = {chest: Trove::Chest, benchmarks: Array(String)}
config = Config.from_yaml File.read ENV["BENCHMARK_CONFIG_PATH"]
chest = config[:chest]

cs = JSON.parse COMPLEX_STRUCTURE.to_json
k = "level1.level2.level3.1.metadata.level4.level5.level6.note"
v = "This is six levels deep"

Benchmark.ips do |b|
  b.report "set+delete" do
    chest.transaction do |transaction|
      transaction.delete transaction << cs
    end
  end
end

i = Trove::ObjectId.random

macro run_read_benchmarks(type)
  Benchmark.ips do |b|
    chest.transaction do |transaction|
      b.report "#{{{type}}}: has key" do
        raise "Can not get" unless transaction.has_key? i, k
      end
      b.report "#{{{type}}}: has key (only simple)" do
        raise "Can not get" unless transaction.has_key! i, k
      end
      b.report "#{{{type}}}: get full" do
        raise "Can not get" if transaction.get(i) != cs
      end
      b.report "#{{{type}}}: get field" do
        raise "Can not get" if transaction.get(i, k) != v
      end
      b.report "#{{{type}}}: get field (only simple)" do
        raise "Can not get" if transaction.get!(i, k) != v
      end
    end
  end
  Benchmark.ips do |b|
    n = 10**4 - 1
    chest.transaction do |transaction|
      (1..n).each { transaction << cs }
      b.report "#{{{type}}}: get one oid from index" do
        transaction.where([{k, v}]) { |ii| break }
      end
      b.report "#{{{type}}}: get #{n + 1} oids from index" do
        g = 0
        transaction.where([{k, v}]) { |ii| g += 1 }
        raise "#{g} != #{n + 1}" if g != n + 1
      end
    end
  end
end

if config[:benchmarks].includes? "in-memory"
  chest.transaction { |transaction| i = transaction << cs }
  run_read_benchmarks "in-memory"
end

if config[:benchmarks].includes? "on-disk"
  chest.clear
  chest.transaction { |transaction| i = transaction << cs }
  chest.checkpoint
  run_read_benchmarks "on-disk"
end
