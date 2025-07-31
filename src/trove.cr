require "json"
require "uuid"
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
                              value: {dv: String}},  #       value
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
    property intx = false
    getter env : Env

    def initialize(@env : Env)
    end

    protected def oid : Oid
      b = UUID.v7.bytes.to_slice
      {IO::ByteFormat::BigEndian.decode(UInt64, b[0..7]),
       IO::ByteFormat::BigEndian.decode(UInt64, b[8..15])}
    end

    protected def digest(s : Bytes)
      d = LibXxhash.xxhash128 s, s.size, 0
      {d.high64, d.low64}
    end

    def oids(&)
      @env.from({oi0: 0_u64, oi1: 0_u64}) { |o| yield({o[:oi0], o[:oi1]}) }
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

    protected def encode(v : I) : String
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

    protected def decode(s : String) : I
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
      d = digest (pp[:b] + oe).to_slice
      @env << {ipv0: d[0], ipv1: d[1], ipi: pp[:i], ii0: i[0], ii1: i[1]}
      @env << {upv0: d[0], upv1: d[1], ui0: i[0], ui1: i[1]}
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

    protected def delete(i : Oid, p : String, ve : String)
      transaction do |ttx|
        ttx.env.delete({di0: i[0], di1: i[1], dp: p})
        pp = partition p
        dg = digest (pp[:b] + ve).to_slice
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
      dg = digest (pp[:b] + encode v).to_slice
      @env.from({ipv0: dg[0], ipv1: dg[1], ipi: pp[:i], ii0: 0_u64, ii1: 0_u64}) do |i|
        break unless {i[:ipv0], i[:ipv1]} == dg
        yield({i[:ii0], i[:ii1]})
      end
    end

    def unique(p : String, v : I)
      dg = digest (p + encode v).to_slice
      u = @env[{upv0: dg[0], upv1: dg[1]}]?
      return nil unless u
      {u[:ui0], u[:ui1]}
    end
  end
end
