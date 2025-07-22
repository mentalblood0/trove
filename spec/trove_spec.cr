require "spec"

require "../src/trove"
require "./common.cr"

describe Trove do
  opts = Sophia::H{"compression"      => "zstd",
                   "compaction.cache" => 2_i64 * 1024 * 1024 * 1024}
  chest = Trove::Chest.new Trove::Env.new Sophia::H{"sophia.path" => "/tmp/trove"}, {d: Sophia::H.new, i: opts}

  [
    "string",
    1234_i64,
    1234.1234_f64,
    -1234_i64,
    -1234.1234_f64,
    0_i64,
    0.0_f64,
    true,
    false,
    nil,
    {"key" => "value"},
    {"a" => "b", "c" => "d"},
    {"a" => {"b" => "c"}},
    {"a" => {"b" => {"c" => "d"}}},
    {"a" => {"b" => {"c" => "d"}}},
    ["a", "b", "c"],
    ["a"],
    COMPLEX_STRUCTURE,
  ].each do |o|
    it "add+get+where+delete #{o}" do
      j = o.to_json
      p = JSON.parse j
      i = chest << p
      chest.get(i).should eq o

      case o
      when String, Int64, Float64, Bool, Nil
        chest.where("", o) { |ii| ii.should eq i }
      when Array(String)
        o.each_with_index { |v, k| chest.where(k.to_s, v) { |ii| ii.should eq i } }
      when Hash(String, String)
        o.each { |k, v| chest.where(k.to_s, v) { |ii| ii.should eq i } }
      when COMPLEX_STRUCTURE
        puts "index test"
        chest.get(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
        chest.get!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
        chest.where("level1.level2.level3.1.metadata.level4.level5.level6.note", "This is six levels deep") { |ii| break }
      end

      chest.delete i
      chest.get(i).should eq nil
    end
  end
end
