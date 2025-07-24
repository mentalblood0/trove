require "json"
require "uuid"

require "sophia"

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
                                    ii1: UInt64}}}

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
      oe = case o
           when H
             o.each { |k, v| set i, p.empty? ? k.to_s : "#{p}.#{k}", v.raw }
             return
           when AA
             o.each_with_index { |v, k| set i, p.empty? ? k.to_s : "#{p}.#{k}", v.raw }
             return
           when String
             "s#{o}"
           when Int64
             "i#{o}"
           when Float64
             "f#{o}"
           when true
             "T"
           when false
             "F"
           when nil
             ""
           else
             raise "Can not encode #{o}"
           end
      @env << {di0: i[0], di1: i[1], dp: p, dv: oe}
      @env << {ip: p, iv: oe, ii0: i[0], ii1: i[1]}
    end

    def set(i : Oid, p : String, o : A)
      transaction do |ttx|
        ttx.delete i, p
        ttx.set! i, p, o
      end
    end

    def set!(i : Oid, p : String, o : A)
      transaction do |ttx|
        ttx.delete! i, p
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
        if ah.has_key? "0"
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
        ps = p.split '.'
        c = r

        ps.each_with_index do |k, i|
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
        ttx.env.from({di0: i[0], di1: i[1], dp: p}) do |d|
          break unless d[:di0] == i[0] && d[:di1] == i[1] && d[:dp].starts_with? p
          ttx.env.delete({di0: d[:di0], di1: d[:di1], dp: d[:dp]})
          ttx.env.delete({ip: d[:dp], iv: d[:dv], ii0: d[:di0], ii1: d[:di1]})
        end
      end
    end

    def delete!(i : Oid, p : String = "")
      transaction do |ttx|
        ttx.env.delete({ip: p, iv: (ttx.env[{di0: i[0], di1: i[1], dp: p}]?.not_nil![:dv] rescue return), ii0: i[0], ii1: i[1]})
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

    def where(p : String, v : I, &)
      ve = encode v
      @env.from({ip: p, iv: ve, ii0: 0_u64, ii1: 0_u64}) do |i|
        break unless i[:ip].starts_with?(p) && i[:iv] == ve
        yield({i[:ii0], i[:ii1]})
      end
    end

    def where!(p : String, v : I, &)
      ve = encode v
      @env.from({ip: p, iv: ve, ii0: 0_u64, ii1: 0_u64}) do |i|
        break unless i[:ip] == p && i[:iv] == ve
        yield({i[:ii0], i[:ii1]})
      end
    end
  end
end
