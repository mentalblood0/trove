require "spec"

require "../src/trove"
require "./common.cr"

describe Trove do
  chest = Trove::Chest.from_yaml File.read "spec/config.yml"

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

    chest.objects.should eq [{oid, parsed}]

    # indexes accept multiple present as well as multiple absent fields

    chest.where({"dict.boolean" => false}).should eq [oid]
    chest.where({"dict.boolean" => true}).should eq [] of Trove::Oid
    chest.where({"dict.hello" => "number"}).should eq [oid]
    chest.where({"dict.boolean" => false,
                 "dict.hello"   => "number"}).should eq [oid]
    chest.where({"dict.boolean" => true,
                 "dict.hello"   => "number"}).should eq [] of Trove::Oid
    chest.where({"dict.boolean" => false}, {"dict.hello" => "number"}).should eq [] of Trove::Oid
    chest.where({"dict.boolean" => false}, {"dict.hello" => "lalala"}).should eq [oid]

    chest.delete! oid, "dict.hello"
    chest.get(oid, "dict.hello").should eq ["number", 42, -4.2, 0.0]

    chest.delete! oid, "dict.hello.2"
    chest.get(oid, "dict.hello.2").should eq nil
    chest.get(oid, "dict.hello").should eq ["number", 42, 0.0]
    chest.where({"dict.hello" => -4.2}).should eq [] of Trove::Oid

    chest.delete oid, "dict.hello"
    chest.get(oid, "dict.hello").should eq nil
    chest.get(oid, "dict").should eq({"boolean" => false})

    chest.delete! oid, "dict.boolean"
    chest.where({"dict.boolean" => false}).should eq [] of Trove::Oid
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
    chest.where({"dict.boolean" => false}).should eq [] of Trove::Oid
    chest.where({"dict" => false}).should eq [] of Trove::Oid
  end

  it "can get indexes of simple elements of root array" do
    p = JSON.parse (0..10).map { |n| n }.to_json
    i = chest << p
    (0..10).each { |n| chest.index(i, "", n.to_i64).should eq n.to_u32 }
    chest.delete i
  end

  it "can get indexes of simple elements of non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| n }}).to_json
    i = chest << p
    (0..10).each { |n| chest.index(i, "l", n.to_i64).should eq n.to_u32 }
    chest.delete i
  end

  it "can get first and last simple elements of root array" do
    p = JSON.parse (0..10).map { |n| n }.to_json
    i = chest << p
    chest.first(i, "").should eq({"0", 0})
    chest.last(i, "").should eq({"10", 10})
    chest.delete i
  end

  it "can get first and last simple elements of non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| n }}).to_json
    i = chest << p
    chest.first(i, "l").should eq({"l.0", 0})
    chest.last(i, "l").should eq({"l.10", 10})
    chest.delete i
  end

  it "can get first and last complex elements of root array" do
    p = JSON.parse (0..10).map { |n| {"n" => n} }.to_json
    i = chest << p
    chest.first(i, "").should eq({"0", {"n" => 0}})
    chest.last(i, "").should eq({"10", {"n" => 10}})
    chest.delete i
  end

  it "can get first and last complex elements of non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| {"n" => n} }}).to_json
    i = chest << p
    chest.first(i, "l").should eq({"l.0", {"n" => 0}})
    chest.last(i, "l").should eq({"l.10", {"n" => 10}})
    chest.delete i
  end

  it "can push simple elements to root array" do
    p = JSON.parse (0..10).map { |n| n }.to_json
    i = chest << p
    pi = chest.push i, "", [p]
    pi.should eq 11
    chest.get(i, "11").should eq p
    chest.delete i
  end

  it "can push simple elements to non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| n }}).to_json
    i = chest << p
    pi = chest.push i, "l", [p]
    pi.should eq 11
    chest.get(i, "l.11").should eq p
    chest.delete i
  end

  it "can push complex elements to root array" do
    p = JSON.parse (0..10).map { |n| {"n" => n} }.to_json
    i = chest << p
    pi = chest.push i, "", [p]
    pi.should eq 11
    chest.get(i, "11").should eq p
    chest.delete i
  end

  it "can push complex elements to non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| {"n" => n} }}).to_json
    i = chest << p
    pi = chest.push i, "l", [p]
    pi.should eq 11
    chest.get(i, "l.11").should eq p
    chest.delete i
  end

  it "can push to empty array", focus: true do
    p = JSON::Any.new 11
    i = Trove::Oid.random
    pi = chest.push i, "", [p]
    pi.should eq 0
    chest.get(i, "0").should eq p
    chest.delete i
  end

  it "do not add repeated elements when adding arrays" do
    p = JSON.parse %([2, 2, 1])
    i = chest << p
    (chest.get i).should eq JSON.parse [2, 1].to_json
    chest.delete i
  end

  it "do not push repeated elements to arrays" do
    p = JSON.parse (1..3).map { |n| n }.to_json
    i = chest << p
    (chest.push i, "", [1, 20, 3, 20].map { |n| JSON::Any.new n }).should eq 3
    chest.get(i, "").should eq JSON::Any.new [1, 2, 3, 20].map { |n| JSON::Any.new n }
    chest.delete i
  end

  it "do not set! repeated elements to arrays" do
    p = JSON.parse (1..3).map { |n| n }.to_json
    i = chest << p
    [1, 20, 3, 20].each_with_index { |n, nn| chest.set! i, (3 + nn).to_s, JSON::Any.new n }
    chest.get(i, "").should eq JSON::Any.new [1, 2, 3, 20].map { |n| JSON::Any.new n }
    chest.delete i
  end

  it "supports dots in keys" do
    p = JSON.parse %({"a.b.c": 1})
    i = chest << p
    chest.get(i).should eq p
    chest.delete i
  end

  it "supports removing first array element" do
    p = JSON.parse %(["a", "b", "c"])
    i = chest << p
    chest.delete! i, "0"
    chest.get(i).should eq ["b", "c"]
    chest.set! i, "k", JSON.parse %("a")
    chest.get(i).should eq({"k" => "a", "1" => "b", "2" => "c"})
    chest.delete i
  end

  it "supports indexing large values" do
    size = 2 ** 16
    v = ["a" * size]
    j = v.to_json
    p = JSON.parse j
    i = chest << p
    chest.where({"" => v.first}).should eq [i]
    chest.get(i).should eq v
    chest.delete i
  end

  it "distinguishes in key/value pairs with same concatenation result" do
    i0 = chest << JSON.parse %({"as": "a"})
    i1 = chest << JSON.parse %({"a": "sa"})
    chest.where({"as" => "a"}).should eq [i0]
    chest.where({"a" => "sa"}).should eq [i1]
    chest.delete i0
    chest.delete i1
  end

  it "can dump and load data" do
    # dump is gzip compressed json lines of format
    # {"oid": <object identifier>, "data": <object>}

    o0 = {"a" => "b"}
    o1 = COMPLEX_STRUCTURE
    i0 = chest << JSON.parse o0.to_json
    i1 = chest << JSON.parse o1.to_json

    dump = IO::Memory.new
    chest.dump dump

    chest.delete i0
    chest.delete i1
    chest.get(i0).should eq nil
    chest.get(i1).should eq nil

    dump.rewind
    chest.load dump

    (Set.new chest.objects).should eq Set.new [{i0, o0}, {i1, o1}]
    chest.get(i0).should eq o0
    chest.get(i1).should eq o1
    chest.delete i0
    chest.delete i1
  end

  it "correctly gets arrays with >9 elements" do
    o = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    i = chest << JSON.parse o.to_json
    chest.get(i).should eq o
    chest.delete i
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
        chest.where({"" => o}).should eq [i]
        chest.has_key!(i).should eq true
      when Array
        o.each_with_index do |v, k|
          chest.has_key!(i, k.to_s).should eq true
          chest.where({"" => v}).should eq [i]
        end
      when Hash(String, String)
        o.each do |k, v|
          chest.has_key!(i, k).should eq true
          chest.where({k.to_s => v}).should eq [i]
        end
      when COMPLEX_STRUCTURE
        chest.has_key!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq true
        chest.get(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
        chest.get!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
        chest.where({"level1.level2.level3.1.metadata.level4.level5.level6.note" => "This is six levels deep"}).should eq [i]
      end

      chest.delete i
      chest.has_key!(i).should eq false
      chest.get(i).should eq nil
    end
  end
end
