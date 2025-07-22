# 🏴‍☠️ trove

Arbitrary JSON-structured data, all-indexing storage backed with [Sophia](https://github.com/pmwkaa/sophia)

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     trove:
       github: mentalblood0/trove
   ```

2. Run `shards install`

## Usage

See also spec/trove_spec.cr

```crystal
require "spec" # for demonstration

require "trove"

describe Trove do
  opts = Sophia::H{"compression" => "zstd"}
  chest = Trove::Chest.new Trove::Env.new Sophia::H{"sophia.path" => "/tmp/trove"}, {d: opts, i: opts}

  it "example" do
    parsed = JSON.parse %({
                            "string": {
                              "hello": ["number", 42, -4.2, 0.0],
                              "boolean": false
                            },
                            "null": null,
                            "array": [1, ["two", false], [null]]
                          })
    oid = chest << parsed

    chest.get(oid).should eq parsed
    chest.get(oid, "string").should eq({"hello" => ["number", 42, -4.2, 0.0], "boolean" => false})
    chest.get(oid, "string.hello").should eq ["number", 42, -4.2, 0.0]
    chest.get(oid, "string.boolean").should eq false

    # get! is faster than get, but expects simple value
    # because under the hood all values in trove are simple
    # and get! just gets value by key, without range scan

    chest.get!(oid, "string.boolean").should eq false
    chest.get!(oid, "string").should eq nil
    chest.get!(oid, "string.hello.0").should eq "number"
    chest.get!(oid, "string.hello.1").should eq 42
    chest.get!(oid, "string.hello.2").should eq -4.2
    chest.get!(oid, "string.hello.3").should eq 0.0
    chest.get!(oid, "null").should eq nil
    chest.get!(oid, "nonexistent.key").should eq nil
    chest.get(oid, "array").should eq [1, ["two", false], [nil]]
    chest.get!(oid, "array.0").should eq 1
    chest.get(oid, "array.1").should eq ["two", false]
    chest.get!(oid, "array.1.0").should eq "two"
    chest.get!(oid, "array.1.1").should eq false
    chest.get(oid, "array.2").should eq [nil]
    chest.get!(oid, "array.2.0").should eq nil

    chest.has_key?(oid, "null").should eq true
    chest.has_key!(oid, "null").should eq true
    chest.has_key?(oid, "string").should eq true
    chest.has_key!(oid, "string").should eq false
    chest.has_key?(oid, "nonexistent.key").should eq false
    chest.has_key!(oid, "nonexistent.key").should eq false

    # indexes work for simple values

    oids = [] of Trove::Oid
    chest.where!("string.boolean", false) { |i| oids << i }
    oids.should eq [oid]

    chest.where!("string.boolean", true) { |i| raise "Who is there?" }

    oids = [] of Trove::Oid
    chest.where!("string.hello.0", "number") { |i| oids << i }
    oids.should eq [oid]

    chest.delete!(oid, "string.hello")
    chest.get(oid, "string.hello").should eq ["number", 42, -4.2, 0.0]

    chest.delete!(oid, "string.hello.2")
    chest.get(oid, "string.hello.2").should eq nil
    chest.get(oid, "string.hello").should eq ["number", 42, 0.0]

    chest.delete(oid, "string.hello")
    chest.get(oid, "string.hello").should eq nil
    chest.get(oid, "string").should eq({"boolean" => false})

    chest.delete!(oid, "string.boolean")
    chest.get(oid, "string").should eq nil
    chest.get(oid).should eq({"null" => nil, "array" => [1, ["two", false], [nil]]})

    chest.delete(oid)
    chest.get(oid).should eq nil
    chest.get(oid, "null").should eq nil
  end
end
```
