require "spec"

require "../src/trove"

describe Trove do
  opts = Sophia::H{"compression"      => "zstd",
                   "compaction.cache" => 2_i64 * 1024 * 1024 * 1024}
  chest = Trove::Chest.new Trove::Env.new Sophia::H{"sophia.path" => "/tmp/trove"}, {d: Sophia::H.new, i: opts}

  [
    "string",
    1234,
    1234.1234,
    -1234,
    -1234.1234,
    0,
    0.0,
    true,
    false,
    nil,
    {"key" => "value"},
    {"a" => {"b" => "c"}},
    {"a" => {"b" => {"c" => "d"}}},
    {"a" => {"b" => {"c" => "d"}}},
    ["a", "b", "c"],
    {
      "level1" => {
        "name"     => "Root Object",
        "isActive" => true,
        "priority" => nil,
        "level2"   => {
          "description" => "Second level nested object",
          "tags"        => ["nested", "test", "json"],
          "count"       => 42,
          "level3"      => [
            {
              "id"       => 1,
              "values"   => [true, false, nil],
              "metadata" => {
                "created"  => "2023-01-01T00:00:00Z",
                "modified" => "2023-01-15T12:30:45Z",
                "level4"   => {
                  "coordinates" => {
                    "x" => 12.34,
                    "y" => -56.78,
                    "z" => 90.12,
                  },
                  "isValid" => false,
                },
              },
            },
            {
              "id"       => 2,
              "values"   => [1, 2, 3, 4, 5],
              "metadata" => {
                "created"  => "2023-02-01T00:00:00Z",
                "modified" => nil,
                "level4"   => {
                  "coordinates" => {
                    "x" => -98.76,
                    "y" => 54.32,
                    "z" => 10.98,
                  },
                  "isValid" => true,
                  "level5"  => {
                    "description" => "Deeply nested object",
                    "flags"       => [true, true, false, true],
                    "level6"      => {
                      "final" => true,
                      "note"  => "This is six levels deep",
                    },
                  },
                },
              },
            },
          ],
        },
        "threshold"     => 0.0001,
        "escapeChars"   => "Special chars: \\\" \b \f \n \r \t",
        "unicodeChars"  => "日本語 Español ελληνικά",
        "largeNumber"   => 987654321098,
        "preciseNumber" => 0.123456789012345678,
      },
      "otherProperties" => [
        {
          "type"  => "string",
          "value" => "Simple string",
        },
        {
          "type"  => "number",
          "value" => 123.456,
        },
        {
          "type"  => "boolean",
          "value" => false,
        },
        {
          "type"  => "null",
          "value" => nil,
        },
        {
          "type"  => "array",
          "value" => ["a", "b", "c"],
        },
        {
          "type"  => "object",
          "value" => {
            "key" => "value",
          },
        },
      ],
    },
  ].each do |o|
    it "add #{o}" do
      j = o.to_json
      i = chest << JSON.parse j
      r = chest[i]?
      r.should eq o
    end
  end
end
