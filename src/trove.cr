require "json"
require "uuid"

require "sophia"

@[Link(ldflags: "#{__DIR__}/xxhash/xxhash.o")]
lib LibXxhash
  fun xxhash128 = XXH128(input : Void*, size : LibC::SizeT, seed : LibC::ULongLong) : XXH128_Hash

  struct XXH128_Hash
    low64, high64 : UInt64
  end
end

module Trove
  alias Oid = {UInt64, UInt64}
  alias A = JSON::Any
  alias H = Hash(String, A)
  alias AA = Array(A)

  Sophia.define_env Env, {d: {key: {di0: UInt64,
                                    di1: UInt64,
                                    dp: String},
                              value: {dv: String}},
                          i: {key: {ip: String,
                                    iv: String,
                                    ii0: UInt64,
                                    ii1: UInt64}},
                          u: {key: {up: String,
                                    uv: String},
                              value: {ui0: UInt64,
                                      ui1: UInt64}},
                          o: {key: {oi0: UInt64,
                                    oi1: UInt64}}}

  class Chest
    property intx = false
    getter env : Env

    def initialize(@env : Env)
    end

    protected def oid : Oid
      b = UUID.v7.bytes.to_slice
      {IO::ByteFormat::BigEndian.decode(UInt64, b[0..7]),
       IO::ByteFormat::BigEndian.decode(UInt64, b[8..15])}
    end

    def oids(&)
      @env.from({oi0: 0_u64, oi1: 0_u64}) { |o| yield({o[:oi0], o[:oi1]}) }
    end

    def transaction(&)
      if @intx
        yield self if @intx
      else
        @env.transaction do |tx|
          r = Chest.new tx
          r.intx = true
          yield r
        end
      end
    end

    protected def decode(s : String)
      return nil if s.empty?
      case s[0]
      when 's'
        s[1..]
      when 'i'
        s[1..].to_i64
      when 'f'
        s[1..].to_f
      when 'T'
        true
      when 'F'
        false
      end
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
      iv = index_value p.bytesize + 16, oe
      @env << {ip: p, iv: iv, ii0: i[0], ii1: i[1]}
      @env << {up: p, uv: iv, ui0: i[0], ui1: i[1]}
    end

    def set(i : Oid, p : String, o : A)
      transaction do |ttx|
        ttx.delete i, p
        ttx.env << {oi0: i[0], oi1: i[1]}
        ttx.set i, p, o.raw
      end
    end

    def set!(i : Oid, p : String, o : A)
      transaction do |ttx|
        ttx.delete! i, p
        ttx.env << {oi0: i[0], oi1: i[1]}
        ttx.set i, p, o.raw
      end
    end

    def <<(o : A)
      i = oid
      set i, "", o
      i
    end

    protected def h2a(a : A) : A
      if ah = a.as_h?
        if ah.keys.all? { |k| k.to_u64 rescue nil }
          vs = ah.values
          return A.new AA.new(ah.size) { |i| h2a vs[i] }
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
        break unless d[:di0] == i[0] && d[:di1] == i[1] && d[:dp].starts_with? p
        flat[d[:dp].lchop(p).lchop('.')] = A.new decode d[:dv]
      end
      return nil if flat.size == 0
      return flat[""] if flat.has_key? ""
      h2a A.new nest flat
    end

    def get!(i : Oid, p : String)
      decode @env[{di0: i[0], di1: i[1], dp: p}]?.not_nil![:dv] rescue nil
    end

    def delete(i : Oid, p : String = "")
      transaction do |ttx|
        ttx.env.delete({oi0: i[0], oi1: i[1]}) if p.empty?
        ttx.env.from({di0: i[0], di1: i[1], dp: p}) do |d|
          break unless d[:di0] == i[0] && d[:di1] == i[1] && d[:dp].starts_with? p
          ttx.env.delete({di0: d[:di0], di1: d[:di1], dp: d[:dp]})
          iv = index_value d[:dp].bytesize + 16, d[:dv]
          ttx.env.delete({ip: d[:dp], iv: iv, ii0: d[:di0], ii1: d[:di1]})
          ttx.env.delete({up: d[:dp], uv: iv})
        end
      end
    end

    def delete!(i : Oid, p : String = "")
      transaction do |ttx|
        v = ttx.env[{di0: i[0], di1: i[1], dp: p}]?.not_nil![:dv] rescue return
        ttx.env.delete({ip: p, iv: v, ii0: i[0], ii1: i[1]})
        ttx.env.delete({up: p, uv: v})
        ttx.env.delete({di0: i[0], di1: i[1], dp: p})
      end
    end

    alias I = String | Int64 | Float64 | Bool | Nil

    protected def encode(v : I)
      case v
      when String
        "s#{v}"
      when Int64
        "i#{v}"
      when Float64
        "f#{v}"
      when true
        "T"
      when false
        "F"
      when nil
        ""
      else
        raise "Can not encode #{v}"
      end
    end

    protected def index_value(os : Int32, ve : String)
      if ve.bytesize < 1024
        ve
      else
        d = LibXxhash.xxhash128 ve, ve.size, 0
        "%016x%016x" % [d.high64, d.low64]
      end
    end

    protected def where(p : String, strict : Bool, v : I, &)
      iv = index_value p.bytesize + 16, encode v
      @env.from({ip: p, iv: iv, ii0: 0_u64, ii1: 0_u64}) do |i|
        break unless i[:iv] == iv && ((strict && i[:ip] == p) || (!strict && i[:ip].starts_with? p))
        yield({i[:ii0], i[:ii1]})
      end
    end

    def where(p : String, v : I, &)
      where(p, false, v) { |i| yield i }
    end

    def where!(p : String, v : I, &)
      where(p, true, v) { |i| yield i }
    end

    def unique(p : String, v : I)
      u = @env[{up: p, uv: index_value p.bytesize + 16, encode v}]?
      return nil unless u
      {u[:ui0], u[:ui1]}
    end
  end
end
