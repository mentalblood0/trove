require "json"
require "yaml"
require "uuid"
require "compress/gzip"

require "xxhash128"
require "sophia"

module Trove
  alias Oid = {UInt64, UInt64}
  alias A = JSON::Any
  alias H = Hash(String, A)
  alias AA = Array(A)

  Sophia.define_env Env, {d: {key: {di0: UInt64,     # data: oid first 64bits
                                    di1: UInt64,     #       oid last 64bits
                                    dp: String},     #       path
                              value: {dv: Bytes}},   #       value
                          i: {key: {ipv0: UInt64,    # index: path and value digest first 64bits
                                    ipv1: UInt64,    #        path and value digest last 64bit
                                    ipi: UInt32,     #        path array index
                                    ii0: UInt64,     #        oid first 64bits
                                    ii1: UInt64}},   #        oid last 64bits
                          u: {key: {upv0: UInt64,    # unique: path and value digest first 64bits
                                    upv1: UInt64},   #         path and value digest last 64bits
                              value: {ui0: UInt64,   #         oid first 64bits
                                      ui1: UInt64}}, #         oid last 64bits
                          o: {key: {oi0: UInt64,     # oids: first 64bits
                                    oi1: UInt64}}}   #       last 64bits

  class Chest
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter env : Env

    @[YAML::Field(ignore: true)]
    property intx = false

    def initialize(@env : Env)
    end

    protected def new_oid : Oid
      b = UUID.v7.bytes.to_slice
      {IO::ByteFormat::BigEndian.decode(UInt64, b[0..7]),
       IO::ByteFormat::BigEndian.decode(UInt64, b[8..15])}
    end

    protected def digest(s : Bytes)
      d = LibXxhash.xxhash128 s, s.size, 0
      {d.high64, d.low64}
    end

    protected def digest(pb : String, ve : Bytes)
      ds = Bytes.new pb.size + 1 + ve.size
      pb.to_unsafe.copy_to ds.to_unsafe, pb.bytesize
      ve.copy_to ds.to_unsafe + pb.size + 1, ve.size
      digest ds
    end

    def oids(&)
      @env.from({oi0: 0_u64, oi1: 0_u64}) { |o| yield({o[:oi0], o[:oi1]}) }
    end

    def oids
      r = [] of Trove::Oid
      oids { |i| r << i }
      r
    end

    macro mwo
      o = if flat.size == 0
            nil
          elsif flat.has_key? ""
            flat[""]
          else
            h2a A.new nest flat
          end
      oid0 = oid[0]
      oid1 = oid[1]
      gzip.puts({"oid" => pointerof(oid0).as(UInt8*).to_slice(8).hexstring +
                          pointerof(oid1).as(UInt8*).to_slice(8).hexstring,
                 "data" => o}.to_json)
    end

    def dump(io : IO)
      Compress::Gzip::Writer.open(io, Compress::Deflate::BEST_COMPRESSION) do |gzip|
        oid : Oid? = nil
        flat = H.new
        @env.from({di0: 0_u64, di1: 0_u64, dp: ""}) do |d|
          i = {d[:di0], d[:di1]}
          unless i == oid
            if oid
              mwo
              flat.clear
            end
            oid = i
          end
          flat[d[:dp]] = A.new decode d[:dv]
        end
        if oid
          mwo
        end
      end
    end

    def load(io : IO)
      Compress::Gzip::Reader.open(io) do |gzip|
        gzip.each_line do |l|
          p = JSON.parse l.chomp
          oid = p["oid"].as_s.hexbytes
          set(oid.to_unsafe.as(Oid*).value, "", p["data"])
        end
      end
    end

    def transaction(&)
      if @intx
        yield self
      else
        @env.transaction do |tx|
          r = Chest.new tx
          r.intx = true
          yield r
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
          r[1] = v.to_i8!.to_u8
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

    protected def partition(p : String)
      pp = p.rpartition '.'
      {b: pp[0], i: pp[2].to_u32} rescue {b: p, i: 0_u32}
    end

    protected def set(i : Oid, p : String, o : A::Type)
      case o
      when H
        o.each do |k, v|
          ke = k.gsub(".", "\\.")
          set i, p.empty? ? ke : "#{p}.#{ke}", v.raw
        end
        return
      when AA
        o.each_with_index { |v, k| set i, p.empty? ? k.to_s : "#{p}.#{k}", v.raw }
        return
      else
        oe = encode o
      end
      @env << {di0: i[0], di1: i[1], dp: p, dv: oe}
      pp = partition p
      d = digest pp[:b], oe
      @env << {ipv0: d[0], ipv1: d[1], ipi: pp[:i], ii0: i[0], ii1: i[1]}
      @env << {upv0: d[0], upv1: d[1], ui0: i[0], ui1: i[1]}
    end

    def set(i : Oid, p : String, o : A)
      transaction do |ttx|
        if ttx.env.has_key?({oi0: i[0], oi1: i[1]})
          ttx.delete i, p unless p.empty?
        else
          ttx.env << {oi0: i[0], oi1: i[1]}
        end
        ttx.set i, p, o.raw
      end
    end

    protected def deletei(i : Oid, p : String)
      pp = partition p
      dg = digest pp[:b], (@env[{di0: i[0], di1: i[1], dp: p}]?.not_nil![:dv] rescue return)
      @env.delete({ipv0: dg[0], ipv1: dg[1], ipi: pp[:i], ii0: i[0], ii1: i[1]})
      @env.delete({upv0: dg[0], upv1: dg[1]})
    end

    def set!(i : Oid, p : String, o : A)
      transaction do |ttx|
        ttx.deletei i, p
        ttx.env << {oi0: i[0], oi1: i[1]}
        ttx.set i, p, o.raw
      end
    end

    def <<(o : A)
      i = new_oid
      set i, "", o
      i
    end

    protected def h2a(a : A) : A
      if ah = a.as_h?
        if ah.keys.all? { |k| k.to_u32? }
          return A.new ah.values.map { |e| h2a e }
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

    def has_key?(i : Oid, p : String = "")
      @env.from({di0: i[0], di1: i[1], dp: p}) do |d|
        return d[:di0] == i[0] && d[:di1] == i[1] && d[:dp].starts_with? p
      end
      false
    end

    def has_key!(i : Oid, p : String = "")
      @env.has_key?({di0: i[0], di1: i[1], dp: p})
    end

    def get(i : Oid, p : String = "")
      flat = H.new
      @env.from({di0: i[0], di1: i[1], dp: p}) do |d|
        break unless {d[:di0], d[:di1]} == i && d[:dp].starts_with? p
        flat[d[:dp].lchop(p).lchop('.')] = A.new decode d[:dv]
      end
      return nil if flat.size == 0
      return flat[""] if flat.has_key? ""
      h2a A.new nest flat
    end

    def get!(i : Oid, p : String)
      decode @env[{di0: i[0], di1: i[1], dp: p}]?.not_nil![:dv] rescue nil
    end

    protected def delete(i : Oid, p : String, ve : Bytes)
      transaction do |ttx|
        ttx.env.delete({di0: i[0], di1: i[1], dp: p})
        pp = partition p
        dg = digest pp[:b], ve
        ttx.env.delete({ipv0: dg[0], ipv1: dg[1], ipi: pp[:i], ii0: i[0], ii1: i[1]})
        ttx.env.delete({upv0: dg[0], upv1: dg[1]})
      end
    end

    def delete(i : Oid, p : String = "")
      transaction do |ttx|
        ttx.env.delete({oi0: i[0], oi1: i[1]}) if p.empty?
        ttx.env.from({di0: i[0], di1: i[1], dp: p}) do |d|
          break unless {d[:di0], d[:di1]} == i && d[:dp].starts_with? p
          delete i, d[:dp], d[:dv]
        end
      end
    end

    def delete!(i : Oid, p : String = "")
      transaction do |ttx|
        delete i, p, (ttx.env[{di0: i[0], di1: i[1], dp: p}]?.not_nil![:dv] rescue return)
      end
    end

    def where(p : String, v : I, &)
      pp = partition p
      dg = digest pp[:b], encode v
      @env.from({ipv0: dg[0], ipv1: dg[1], ipi: pp[:i], ii0: 0_u64, ii1: 0_u64}) do |i|
        break unless {i[:ipv0], i[:ipv1]} == dg
        yield({i[:ii0], i[:ii1]})
      end
    end

    def where(p : String, v : I)
      r = [] of Trove::Oid
      where(p, v) { |i| r << i }
      r
    end

    def unique(p : String, v : I)
      dg = digest p, encode v
      u = @env[{upv0: dg[0], upv1: dg[1]}]?
      return nil unless u
      {u[:ui0], u[:ui1]}
    end
  end
end
