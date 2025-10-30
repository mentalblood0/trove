require "spec"

require "../src/trove"
require "./common.cr"

struct Slice(T)
  def pretty_print(pp : PrettyPrint)
    pp.text "Bytes[#{self.hexstring}]"
  end
end

alias Config = {chest: Trove::Chest}
config = Config.from_yaml File.read ENV["SPEC_CONFIG_PATH"]
chest = config[:chest]

Spec.after_each { chest.clear }

describe Trove do
  it "example" do
    parsed = JSON.parse %({
                            "dict": {
                              "hello": ["number", 42, -4.2, 0.0],
                              "boolean": false
                            },
                            "null": null,
                            "array": [1, ["two", false], [null]]
                          })

    chest.transaction do |transaction|
      object_id = transaction << parsed

      transaction.get(object_id).should eq parsed
      transaction.get(object_id, "dict").should eq parsed["dict"]
      transaction.get(object_id, "dict.hello").should eq parsed["dict"]["hello"]
      transaction.get(object_id, "dict.boolean").should eq parsed["dict"]["boolean"]

      # get! is faster than get, but expects simple value
      # because under the hood all values in trove are simple
      # and get! just gets value by key, without range scan

      transaction.get!(object_id, "dict.boolean").should eq parsed["dict"]["boolean"]
      transaction.get!(object_id, "dict").should eq nil
      transaction.get!(object_id, "dict.hello.0").should eq parsed["dict"]["hello"][0]
      transaction.get!(object_id, "dict.hello.1").should eq parsed["dict"]["hello"][1]
      transaction.get!(object_id, "dict.hello.2").should eq parsed["dict"]["hello"][2]
      transaction.get!(object_id, "dict.hello.3").should eq parsed["dict"]["hello"][3]
      transaction.get!(object_id, "null").should eq nil
      transaction.get!(object_id, "nonexistent.key").should eq nil
      transaction.get(object_id, "array").should eq parsed["array"]
      transaction.get!(object_id, "array.0").should eq parsed["array"][0]
      transaction.get(object_id, "array.1").should eq parsed["array"][1]
      transaction.get!(object_id, "array.1.0").should eq parsed["array"][1][0]
      transaction.get!(object_id, "array.1.1").should eq parsed["array"][1][1]
      transaction.get(object_id, "array.2").should eq parsed["array"][2]
      transaction.get!(object_id, "array.2.0").should eq parsed["array"][2][0]

      transaction.has_key?(object_id, "null").should eq true
      transaction.has_key!(object_id, "null").should eq true
      transaction.has_key?(object_id, "dict").should eq true
      transaction.has_key!(object_id, "dict").should eq false
      transaction.has_key?(object_id, "nonexistent.key").should eq false
      transaction.has_key!(object_id, "nonexistent.key").should eq false

      transaction.objects.should eq [{object_id, parsed}]

      # indexes accept multiple present as well as multiple absent fields

      transaction.where([{"dict.boolean", false}]).should eq [object_id]
      transaction.where([{"dict.boolean", true}]).should eq [] of Trove::ObjectId
      transaction.where([{"dict.hello", "number"}]).should eq [object_id]
      transaction.where([{"dict.boolean", false},
                         {"dict.hello", "number"}]).should eq [object_id]
      transaction.where([{"dict.boolean", true},
                         {"dict.hello", "number"}]).should eq [] of Trove::ObjectId
      transaction.where([{"dict.boolean", false}], [{"dict.hello", "number"}]).should eq [] of Trove::ObjectId
      transaction.where([{"dict.boolean", false}], [{"dict.hello", "lalala"}]).should eq [object_id]

      transaction.delete! object_id, "dict.hello"
      transaction.get(object_id, "dict.hello").should eq ["number", 42, -4.2, 0.0]

      transaction.delete! object_id, "dict.hello.2"
      transaction.get(object_id, "dict.hello.2").should eq nil
      transaction.get(object_id, "dict.hello").should eq ["number", 42, 0.0]
      transaction.where([{"dict.hello", -4.2}]).should eq [] of Trove::ObjectId

      transaction.delete object_id, "dict.hello"
      transaction.get(object_id, "dict.hello").should eq nil
      transaction.get(object_id, "dict").should eq({"boolean" => false})

      transaction.delete! object_id, "dict.boolean"
      transaction.where([{"dict.boolean", false}]).should eq [] of Trove::ObjectId
      transaction.get(object_id, "dict").should eq nil
      transaction.get(object_id).should eq({"null" => nil, "array" => [1, ["two", false], [nil]]})

      transaction.set object_id, "dict", parsed["dict"]
      transaction.get(object_id, "dict").should eq parsed["dict"]
      transaction.set object_id, "dict.boolean", JSON.parse %({"a": "b", "c": 4})
      transaction.get(object_id, "dict.boolean").should eq({"a" => "b", "c" => 4})

      # set! works when overwriting simple values

      transaction.set! object_id, "dict.null", parsed["array"]
      transaction.get(object_id, "dict.null").should eq parsed["array"]

      transaction.delete object_id
      transaction.get(object_id).should eq nil
      transaction.get(object_id, "null").should eq nil

      transaction.set object_id, "", parsed

      transaction.delete object_id
      transaction.get(object_id).should eq nil
      transaction.get(object_id, "null").should eq nil
      transaction.where([{"dict.boolean", false}]).should eq [] of Trove::ObjectId
      transaction.where([{"dict", false}]).should eq [] of Trove::ObjectId
    end
  end

  it "can get indexes of simple elements of root array" do
    p = JSON.parse (0..10).map { |n| n }.to_json
    chest.transaction do |transaction|
      i = transaction << p
      (0..10).each { |n| transaction.index_of(i, "", n.to_i64).should eq n.to_u32 }
      transaction.delete i
    end
  end

  it "can get indexes of simple elements of non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| n }}).to_json
    chest.transaction do |transaction|
      i = transaction << p
      (0..10).each { |n| transaction.index_of(i, "l", n.to_i64).should eq n.to_u32 }
      transaction.delete i
    end
  end

  it "can get first and last simple elements of root array" do
    p = JSON.parse (0..10).map { |n| n }.to_json
    chest.transaction do |transaction|
      i = transaction << p
      transaction.first(i, "").should eq({"0", 0})
      transaction.last(i, "").should eq({"10", 10})
      transaction.delete i
    end
  end

  it "can get first and last simple elements of non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| n }}).to_json
    chest.transaction do |transaction|
      i = transaction << p
      transaction.first(i, "l").should eq({"l.0", 0})
      transaction.last(i, "l").should eq({"l.10", 10})
      transaction.delete i
    end
  end

  it "can get first and last complex elements of root array" do
    p = JSON.parse (0..10).map { |n| {"n" => n} }.to_json
    chest.transaction do |transaction|
      i = transaction << p
      transaction.first(i, "").should eq({"0", {"n" => 0}})
      transaction.last(i, "").should eq({"10", {"n" => 10}})
      transaction.delete i
    end
  end

  it "can get first and last complex elements of non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| {"n" => n} }}).to_json
    chest.transaction do |transaction|
      i = transaction << p
      transaction.first(i, "l").should eq({"l.0", {"n" => 0}})
      transaction.last(i, "l").should eq({"l.10", {"n" => 10}})
      transaction.delete i
    end
  end

  it "can push simple elements to root array" do
    p = JSON.parse (0..10).map { |n| n }.to_json
    chest.transaction do |transaction|
      i = transaction << p
      pi = transaction.push i, "", [p]
      pi.should eq 11
      transaction.get(i, "11").should eq p
      transaction.delete i
    end
  end

  it "can push simple elements to non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| n }}).to_json
    chest.transaction do |transaction|
      i = transaction << p
      pi = transaction.push i, "l", [p]
      pi.should eq 11
      transaction.get(i, "l.11").should eq p
      transaction.delete i
    end
  end

  it "can push complex elements to root array" do
    p = JSON.parse (0..10).map { |n| {"n" => n} }.to_json
    chest.transaction do |transaction|
      i = transaction << p
      pi = transaction.push i, "", [p]
      pi.should eq 11
      transaction.get(i, "11").should eq p
      transaction.delete i
    end
  end

  it "can push complex elements to non-root array" do
    p = JSON.parse ({"l" => (0..10).map { |n| {"n" => n} }}).to_json
    chest.transaction do |transaction|
      i = transaction << p
      pi = transaction.push i, "l", [p]
      pi.should eq 11
      transaction.get(i, "l.11").should eq p
      transaction.delete i
    end
  end

  it "can push to empty array" do
    p = JSON::Any.new 11
    i = Trove::ObjectId.random
    chest.transaction do |transaction|
      pi = transaction.push i, "", [p]
      pi.should eq 0
      transaction.get(i, "0").should eq p
      transaction.delete i
    end
  end

  it "do not add repeated elements when adding arrays" do
    p = JSON.parse %([2, 2, 1])
    chest.transaction do |transaction|
      i = transaction << p
      (transaction.get i).should eq JSON.parse [2, 1].to_json
      transaction.delete i
    end
  end

  it "do not push repeated elements to arrays" do
    p = JSON.parse (1..3).map { |n| n }.to_json
    chest.transaction do |transaction|
      i = transaction << p
      transaction.push(i, "", [1, 20, 3, 20].map { |n| JSON::Any.new n }).should eq 3
      transaction.get(i, "").should eq JSON::Any.new [1, 2, 3, 20].map { |n| JSON::Any.new n }
      transaction.delete i
    end
  end

  it "do not set! repeated elements to arrays" do
    p = JSON.parse (1..3).map { |n| n }.to_json
    chest.transaction do |transaction|
      i = transaction << p
      [1, 20, 3, 20].each_with_index { |n, nn| transaction.set! i, (3 + nn).to_s, JSON::Any.new n }
      transaction.get(i, "").should eq JSON::Any.new [1, 2, 3, 20].map { |n| JSON::Any.new n }
      transaction.delete i
    end
  end

  it "supports dots in keys" do
    p = JSON.parse %({"a.b.c": 1})
    chest.transaction do |transaction|
      i = transaction << p
      transaction.get(i).should eq p
      transaction.delete i
    end
  end

  it "supports removing first array element" do
    p = JSON.parse %(["a", "b", "c"])
    chest.transaction do |transaction|
      i = transaction << p
      transaction.delete! i, "0"
      transaction.get(i).should eq ["b", "c"]
      transaction.set! i, "k", JSON.parse %("a")
      transaction.get(i).should eq({"k" => "a", "1" => "b", "2" => "c"})
      transaction.delete i
    end
  end

  it "supports indexing large values" do
    size = 2 ** 16
    v = ["a" * size]
    j = v.to_json
    p = JSON.parse j
    chest.transaction do |transaction|
      i = transaction << p
      transaction.where([{"", v.first}]).should eq [i]
      transaction.get(i).should eq v
      transaction.delete i
    end
  end

  it "distinguishes in key/value pairs with same concatenation result" do
    chest.transaction do |transaction|
      i0 = transaction << JSON.parse %({"as": "a"})
      i1 = transaction << JSON.parse %({"a": "sa"})
      transaction.where([{"as", "a"}]).should eq [i0]
      transaction.where([{"a", "sa"}]).should eq [i1]
      transaction.delete i0
      transaction.delete i1
    end
  end

  it "can dump and load data" do
    # dump is gzip compressed json lines of format
    # {"object_id": <object identifier>, "data": <object>}

    o0 = {"a" => "b"}
    o1 = COMPLEX_STRUCTURE
    dump = IO::Memory.new
    chest.transaction do |transaction|
      i0 = transaction << JSON.parse o0.to_json
      i1 = transaction << JSON.parse o1.to_json
      transaction.dump dump
      transaction.delete i0
      transaction.delete i1
      transaction.get(i0).should eq nil
      transaction.get(i1).should eq nil

      dump.rewind
      transaction.load dump

      (Set.new transaction.objects).should eq Set.new [{i0, o0}, {i1, o1}]
      transaction.get(i0).should eq o0
      transaction.get(i1).should eq o1
      transaction.delete i0
      transaction.delete i1
    end
  end

  it "correctly gets arrays with >9 elements" do
    chest.transaction do |transaction|
      o = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
      i = transaction << JSON.parse o.to_json
      transaction.get(i).should eq o
      transaction.delete i
    end
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

      chest.transaction do |transaction|
        i = transaction << p
        transaction.get(i).should eq o

        transaction.has_key?(i).should eq true

        case o
        when String, Int64, Float64, Bool, Nil
          transaction.where([{"", o}]).should eq [i]
          transaction.has_key!(i).should eq true
        when Array
          o.each_with_index do |v, k|
            transaction.has_key!(i, k.to_s).should eq true
            transaction.where([{"", v}]).should eq [i]
          end
        when Hash(String, String)
          o.each do |k, v|
            transaction.has_key!(i, k).should eq true
            transaction.where([{k.to_s, v}]).should eq [i]
          end
        when COMPLEX_STRUCTURE
          transaction.has_key!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq true
          transaction.get(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
          transaction.get!(i, "level1.level2.level3.1.metadata.level4.level5.level6.note").should eq "This is six levels deep"
          transaction.where([{"level1.level2.level3.1.metadata.level4.level5.level6.note", "This is six levels deep"}]).should eq [i]
        end

        transaction.delete i
        transaction.has_key!(i).should eq false
        transaction.get(i).should eq nil
      end
    end
  end
end
