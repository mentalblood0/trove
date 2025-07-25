require "spec"

require "../src/trove"
require "./common.cr"

describe Trove do
  opts = Sophia::H{"compression"      => "zstd",
                   "compaction.cache" => 2_i64 * 1024 * 1024 * 1024}
  chest = Trove::Chest.new Trove::Env.new Sophia::H{"sophia.path" => "/tmp/trove"}, {d: opts, i: opts, u: opts}

  it "example" do
    parsed = JSON.parse %({
                            "dict": {
                              "hello": ["number", 42, -4.2, 0.0],
                              "boolean": false
                            },
                            "null": null,
                            "array": [1, ["two", false], [null]]
                          })
    oid = chest << parsed

    chest.get(oid).should eq parsed
    chest.get(oid, "dict").should eq parsed["dict"]
    chest.get(oid, "dict.hello").should eq parsed["dict"]["hello"]
    chest.get(oid, "dict.boolean").should eq parsed["dict"]["boolean"]

    # get! is faster than get, but expects simple value
    # because under the hood all values in trove are simple
    # and get! just gets value by key, without range scan

    chest.get!(oid, "dict.boolean").should eq parsed["dict"]["boolean"]
    chest.get!(oid, "dict").should eq nil
    chest.get!(oid, "dict.hello.0").should eq parsed["dict"]["hello"][0]
    chest.get!(oid, "dict.hello.1").should eq parsed["dict"]["hello"][1]
    chest.get!(oid, "dict.hello.2").should eq parsed["dict"]["hello"][2]
    chest.get!(oid, "dict.hello.3").should eq parsed["dict"]["hello"][3]
    chest.get!(oid, "null").should eq nil
    chest.get!(oid, "nonexistent.key").should eq nil
    chest.get(oid, "array").should eq parsed["array"]
    chest.get!(oid, "array.0").should eq parsed["array"][0]
    chest.get(oid, "array.1").should eq parsed["array"][1]
    chest.get!(oid, "array.1.0").should eq parsed["array"][1][0]
    chest.get!(oid, "array.1.1").should eq parsed["array"][1][1]
    chest.get(oid, "array.2").should eq parsed["array"][2]
    chest.get!(oid, "array.2.0").should eq parsed["array"][2][0]

    chest.has_key?(oid, "null").should eq true
    chest.has_key!(oid, "null").should eq true
    chest.has_key?(oid, "dict").should eq true
    chest.has_key!(oid, "dict").should eq false
    chest.has_key?(oid, "nonexistent.key").should eq false
    chest.has_key!(oid, "nonexistent.key").should eq false

    # indexes work for simple values

    oids = [] of Trove::Oid
    chest.where!("dict.boolean", false) { |i| oids << i }
    oids.should eq [oid]

    chest.where!("dict.boolean", true) { |i| raise "Who is here?" }

    oids = [] of Trove::Oid
    chest.where!("dict.hello.0", "number") { |i| oids << i }
    oids.should eq [oid]

    # where! looks for exact path match and where only for beginning of,
    # so where is useful for arrays elements for example

    chest.where!("dict.hello", "number") { |i| raise "Who is here?" }
    oids = [] of Trove::Oid
    chest.where("dict.hello", "number") { |i| oids << i }
    oids.should eq [oid]

    # unique is way faster than where,
    # but works correctly only for values that were always unique

    chest.unique("dict.boolean", false).should eq oid

    chest.delete! oid, "dict.hello"
    chest.get(oid, "dict.hello").should eq ["number", 42, -4.2, 0.0]

    chest.delete! oid, "dict.hello.2"
    chest.get(oid, "dict.hello.2").should eq nil
    chest.get(oid, "dict.hello").should eq ["number", 42, 0.0]

    chest.delete oid, "dict.hello"
    chest.get(oid, "dict.hello").should eq nil
    chest.get(oid, "dict").should eq({"boolean" => false})

    chest.delete! oid, "dict.boolean"
    chest.where!("dict.boolean", false) { |i| raise "Who is here?" }
    chest.get(oid, "dict").should eq nil
    chest.get(oid).should eq({"null" => nil, "array" => [1, ["two", false], [nil]]})

    chest.set oid, "dict", parsed["dict"]
    chest.get(oid, "dict").should eq parsed["dict"]
    chest.set oid, "dict.boolean", JSON.parse %({"a": "b", "c": 4})
    chest.get(oid, "dict.boolean").should eq({"a" => "b", "c" => 4})

    # set! works when overwriting simple values

    chest.set! oid, "dict.null", parsed["array"]
    chest.get(oid, "dict.null").should eq parsed["array"]

    s = chest.get(oid, "dict").not_nil!
    begin
      chest.transaction do |tx|
        tx.delete oid, "dict"
        raise "oh no"
        tx << s
      end
    rescue ex
      ex.message.should eq "oh no"
      chest.get(oid, "dict").should eq s
    end

    chest.delete oid
    chest.get(oid).should eq nil
    chest.get(oid, "null").should eq nil

    chest.set oid, "", parsed
    chest.delete oid
    chest.get(oid).should eq nil
    chest.get(oid, "null").should eq nil
    chest.where!("dict.boolean", false) { |i| raise "Who is here?" }
    chest.where("dict", false) { |i| raise "Who is here?" }
  end

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
    [1_i64, 2_i64, 3_i64],
    [1_i64],
    ["a", 1_i64, true, 0.0_f64],
    COMPLEX_STRUCTURE,
  ].each do |o|
    it "add+get+where+delete #{o}" do
      j = o.to_json
      p = JSON.parse j
      i = chest << p
      chest.get(i).should eq o

      chest.has_key?(i).should eq true

      case o
      when String, Int64, Float64, Bool, Nil
        chest.where!("", o) { |ii| ii.should eq i }
        chest.has_key!(i).should eq true
      when Array
        o.each_with_index do |v, k|
          chest.has_key!(i, k.to_s).should eq true
          chest.where!(k.to_s, v) { |ii| ii.should eq i }
        end
      when Hash(String, String)
        o.each do |k, v|
          chest.has_key!(i, k).should eq true
          chest.where!(k.to_s, v) { |ii| ii.should eq i }
        end
      when COMPLEX_STRUCTURE
        chest.has_key!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq true
        chest.get(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
        chest.get!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
        chest.where!("level1.level2.level3.1.metadata.level4.level5.level6.note", "This is six levels deep") { |ii| break }
      end

      chest.delete i
      chest.has_key!(i).should eq false
      chest.get(i).should eq nil
    end
  end
end
