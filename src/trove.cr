require "json"
require "yaml"
require "uuid"
require "compress/gzip"

require "xxhash128"
require "dream"
require "sophia"

module Trove
  alias A = JSON::Any
  alias H = Hash(String, A)
  alias AA = Array(A)

  Sophia.define_env Env, {d: {key: {di0: UInt64,   # data: oid first 64bits
                                    di1: UInt64,   #       oid last 64bits
                                    dp: String},   #       path
                              value: {dv: Bytes}}} #       value

  struct Oid
    alias Value = {UInt64, UInt64}

    getter value : Value

    getter bytes : Bytes {
      r = Bytes.new 16
      IO::ByteFormat::BigEndian.encode value[0], r[0..7]
      IO::ByteFormat::BigEndian.encode value[1], r[8..15]
      r
    }
    getter string : String { bytes.hexstring }

    def initialize(@value)
    end

    def self.random
      from_bytes UUID.v7.bytes.to_slice
    end

    def self.from_bytes(b : Bytes)
      Oid.new({IO::ByteFormat::BigEndian.decode(UInt64, b[0..7]),
               IO::ByteFormat::BigEndian.decode(UInt64, b[8..15])})
    end

    def self.from_string(s : String)
      from_bytes s.hexbytes
    end

    def <=>(other : Oid)
      @value <=> other.value
    end
  end

  def self.digest(s : Bytes)
    d = LibXxhash.xxhash128 s, s.size, 0
    {d.high64, d.low64}
  end

  def self.digest(pb : String, ve : Bytes)
    ds = Bytes.new pb.size + 1 + ve.size
    pb.to_unsafe.copy_to ds.to_unsafe, pb.bytesize
    ve.copy_to ds.to_unsafe + pb.size + 1, ve.size
    digest ds
  end

  class Chest
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter env : Env
    getter index : Dream::Index

    @[YAML::Field(ignore: true)]
    property intx = false

    def initialize(@env, @index)
    end

    macro myo
      if flat.size == 0
        yield({oid, A.new nil})
      elsif flat.has_key? ""
        yield({oid, flat[""]})
      else
        yield({oid, h2a A.new nest flat})
      end
    end

    def objects(&)
      oid : Oid? = nil
      flat = H.new
      @env.from({di0: 0_u64, di1: 0_u64, dp: ""}) do |d|
        i = Oid.new({d[:di0], d[:di1]})
        unless i == oid
          if oid
            myo
            flat.clear
          end
          oid = i
        end
        flat[d[:dp]] = A.new decode d[:dv]
      end
      if oid
        myo
      end
    end

    def objects
      r = [] of {Oid, A}
      objects { |o| r << o }
      r
    end

    def dump(io : IO)
      Compress::Gzip::Writer.open(io, Compress::Deflate::BEST_COMPRESSION) do |gzip|
        objects do |oid, o|
          oid0 = oid.value[0]
          oid1 = oid.value[1]
          gzip.puts({"oid"  => oid.string,
                     "data" => o}.to_json)
        end
      end
    end

    def load(io : IO)
      Compress::Gzip::Reader.open(io) do |gzip|
        gzip.each_line do |l|
          p = JSON.parse l.chomp
          oid = Oid.from_string p["oid"].as_s
          set oid, "", p["data"]
        end
      end
    end

    def transaction(&)
      if @intx
        yield self
      else
        @env.transaction do |tx|
          @index.transaction do |itx|
            r = Chest.new tx, itx
            r.intx = true
            yield r
          end
        end
      end
    end

    alias I = String | Int64 | Float64 | Bool | Nil

    protected def encode(v : I) : Bytes
      case v
      when String
        r = Bytes.new 1 + v.bytesize
        r[0] = {{'s'.ord}}.to_u8!
        v.to_unsafe.copy_to r.to_unsafe + 1, v.bytesize
        r
      when Int64
        if v >= Int8::MIN && v <= Int8::MAX
          r = Bytes.new 1 + 1
          r[0] = {{'1'.ord}}.to_u8!
          IO::ByteFormat::LittleEndian.encode v.to_i8!, r[1..]
          r
        elsif v >= Int16::MIN && v <= Int16::MAX
          r = Bytes.new 1 + 2
          r[0] = {{'2'.ord}}.to_u8!
          IO::ByteFormat::LittleEndian.encode v.to_i16!, r[1..]
          r
        elsif v >= Int32::MIN && v <= Int32::MAX
          r = Bytes.new 1 + 4
          r[0] = {{'4'.ord}}.to_u8!
          IO::ByteFormat::LittleEndian.encode v.to_i32!, r[1..]
          r
        else
          r = Bytes.new 1 + 8
          r[0] = {{'8'.ord}}.to_u8!
          IO::ByteFormat::LittleEndian.encode v, r[1..]
          r
        end
      when Float64
        if v.finite? && v == (vf32 = v.to_f32).to_f64
          r = Bytes.new 1 + 4
          r[0] = {{'3'.ord}}.to_u8!
          IO::ByteFormat::LittleEndian.encode vf32.not_nil!, r[1..]
          r
        else
          r = Bytes.new 1 + 8
          r[0] = {{'5'.ord}}.to_u8!
          IO::ByteFormat::LittleEndian.encode v, r[1..]
          r
        end
      when true  then Bytes.new 1, {{'T'.ord}}.to_u8!
      when false then Bytes.new 1, {{'F'.ord}}.to_u8!
      when nil   then "".to_slice
      else            raise "Can not encode #{v}"
      end
    end

    protected def decode(b : Bytes) : I
      return nil if b.empty?
      case b[0]
      when {{'s'.ord}} then String.new b[1..]
      when {{'1'.ord}} then IO::ByteFormat::LittleEndian.decode(Int8, b[1..]).to_i64!
      when {{'2'.ord}} then IO::ByteFormat::LittleEndian.decode(Int16, b[1..]).to_i64!
      when {{'4'.ord}} then IO::ByteFormat::LittleEndian.decode(Int32, b[1..]).to_i64!
      when {{'8'.ord}} then IO::ByteFormat::LittleEndian.decode(Int64, b[1..])
      when {{'3'.ord}} then IO::ByteFormat::LittleEndian.decode(Float32, b[1..]).to_f64!
      when {{'5'.ord}} then IO::ByteFormat::LittleEndian.decode(Float64, b[1..])
      when {{'T'.ord}} then true
      when {{'F'.ord}} then false
      end
    end

    class IndexBatch
      getter i : Oid
      getter chest : Chest
      getter ds : Array(Oid::Value) = [] of Oid::Value

      def initialize(@i, @chest)
      end

      protected def partition(p : String)
        pp = p.rpartition '.'
        {b: pp[0], i: pp[2].to_u64} rescue {b: p, i: nil}
      end

      def <<(poe : {p: String, oe: Bytes})
        pp = partition poe[:p]
        @ds << (pp[:i] ? (Trove.digest pp[:b], poe[:oe]) : (Trove.digest poe[:p], poe[:oe]))
      end

      def add
        @chest.index.add i.value, @ds
      end

      def delete
        @chest.index.delete i.value, @ds
      end
    end

    protected def set(i : Oid, p : String, o : A::Type, ib : IndexBatch? = nil)
      case o
      when H
        o.each do |k, v|
          ke = k.gsub(".", "\\.")
          set i, p.empty? ? ke : "#{p}.#{ke}", v.raw, ib
        end
        return ib
      when AA
        k = 0_u32
        u = Set(String | Int64 | Float64 | Bool | Nil).new
        o.each do |v|
          raw = v.raw
          case raw
          when String, Int64, Float64, Bool, Nil
            next if u.includes? raw
            u << raw
          end
          kp = k.to_s.rjust 10, '0'
          set i, p.empty? ? kp : "#{p}.#{kp}", raw, ib
          k += 1
        end
        return ib
      else
        oe = encode o
      end
      ib << {p: p, oe: oe} if ib
      @env << {di0: i.value[0], di1: i.value[1], dp: p, dv: oe}
      ib
    end

    def set(i : Oid, p : String, o : A)
      transaction do |ttx|
        ttx.delete i, p
        (ttx.set i, p, o.raw, IndexBatch.new i, self).add
      end
    end

    protected def deletei(i : Oid, p : String)
      ib = IndexBatch.new i, self
      ib << {p: p, oe: (@env[{di0: i.value[0], di1: i.value[1], dp: p}]?.not_nil![:dv] rescue return)}
      ib.delete
    end

    def set!(i : Oid, p : String, o : A)
      transaction do |ttx|
        deletei i, p if has_key! i, p
        (ttx.set i, p, o.raw, IndexBatch.new i, self).add
      end
    end

    def <<(o : A)
      i = Oid.random
      set i, "", o
      i
    end

    protected def h2a(a : A) : A
      if ah = a.as_h?
        if ah.keys.all? { |k| k.to_u32? }
          return A.new ah.keys.sort_by { |s| s.to_u32 }.map { |k| h2a ah[k] }
        else
          ah.each { |k, v| ah[k] = h2a v }
        end
      end
      a
    end

    protected def nest(h : H)
      r = H.new
      h.each do |p, v|
        ps = p.split /(?<!\\)\./
        c = r

        ps.each_with_index do |ke, i|
          k = ke.gsub("\\.", ".")
          if i == ps.size - 1
            c[k] = v
          else
            c[k] ||= A.new H.new
            c = c[k].as_h
          end
        end
      end
      r
    end

    protected def pad(p : String)
      p.gsub(/\b\d{1,9}\b/) { |s| s.rjust(10, '0') }
    end

    protected def unpad(p : String)
      p.gsub(/\b0+(\d+)\b/) { |s| $1 }
    end

    def first(i : Oid, p : String = "")
      p = pad p
      @env.from({di0: i.value[0], di1: i.value[1], dp: "#{p}."}) do |d|
        break unless {d[:di0], d[:di1]} == i.value && d[:dp].starts_with? p
        return {unpad(d[:dp]), decode d[:dv]}
      end
    end

    def last(i : Oid, p : String = "")
      p = pad p
      @env.from({di0: i.value[0], di1: i.value[1], dp: "#{p}.9"}, "<=") do |d|
        break unless {d[:di0], d[:di1]} == i.value && d[:dp].starts_with? p
        return {unpad(d[:dp]), decode d[:dv]}
      end
    end

    def push(i : Oid, p : String, o : A)
      p = pad p
      lp = ((last i, p).not_nil![0] rescue "#{p}.0")
      pp = lp.gsub(/\d+$/) { |s| (s.to_u32 + 1).to_s.rjust 10, '0' }
      set i, pp, o
    end

    def has_key?(i : Oid, p : String = "")
      p = pad p
      @env.from({di0: i.value[0], di1: i.value[1], dp: p}) do |d|
        return d[:di0] == i.value[0] && d[:di1] == i.value[1] && d[:dp].starts_with? p
      end
      false
    end

    def has_key!(i : Oid, p : String = "")
      p = pad p
      @env.has_key?({di0: i.value[0], di1: i.value[1], dp: p})
    end

    def get(i : Oid, p : String = "")
      p = pad p
      flat = H.new
      @env.from({di0: i.value[0], di1: i.value[1], dp: p}) do |d|
        break unless {d[:di0], d[:di1]} == i.value && d[:dp].starts_with? p
        flat[unpad d[:dp].lchop(p).lchop('.')] = A.new decode d[:dv]
      end
      return nil if flat.size == 0
      return flat[""] if flat.has_key? ""
      h2a A.new nest flat
    end

    def get!(i : Oid, p : String)
      p = pad p
      decode @env[{di0: i.value[0], di1: i.value[1], dp: p}]?.not_nil![:dv] rescue nil
    end

    protected def delete(i : Oid, p : String, ve : Bytes, ib : IndexBatch)
      p = pad p
      transaction do |ttx|
        ttx.env.delete({di0: i.value[0], di1: i.value[1], dp: p})
        ib << {p: p, oe: ve}
      end
      ib
    end

    def delete(i : Oid, p : String = "")
      p = pad p
      return unless has_key? i, p
      transaction do |ttx|
        ib = IndexBatch.new i, self
        ttx.env.from({di0: i.value[0], di1: i.value[1], dp: p}) do |d|
          break unless {d[:di0], d[:di1]} == i.value && d[:dp].starts_with? p
          delete i, d[:dp], d[:dv], ib
        end
        ib.delete
      end
    end

    def delete!(i : Oid, p : String = "")
      p = pad p
      return unless has_key! i, p
      transaction do |ttx|
        (delete i, p, (ttx.env[{di0: i.value[0], di1: i.value[1], dp: p}]?.not_nil![:dv] rescue return), IndexBatch.new i, self).delete
      end
    end

    def where(present : Hash(String, I), absent : Hash(String, I) = {} of String => I, from : Oid? = nil, &)
      @index.find(
        present.map { |p, v| Trove.digest pad(p), encode v },
        absent.map { |p, v| Trove.digest pad(p), encode v },
        from ? from.value : nil) { |o| yield Oid.new o }
    end

    def where(present : Hash(String, I), absent : Hash(String, I) = {} of String => I, from : Oid? = nil) : Array(Oid)
      r = [] of Oid
      where(present, absent, from) { |i| r << i }
      r
    end
  end
end
