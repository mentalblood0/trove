require "json"
require "uuid"

require "sophia"

module Trove
  alias Oid = {UInt64, UInt64}
  alias DataRecord = {di0: UInt64, di1: UInt64, dp: String, dv: String}
  alias IndexRecord = {ip: String, iv: String, ii0: UInt64, ii1: UInt64}
  alias Record = DataRecord | IndexRecord
  alias A = JSON::Any
  alias H = Hash(String, A)
  alias AA = Array(A)

  Sophia.define_env Env, {d: {key: {di0: UInt64,
                                    di1: UInt64,
                                    dp: String},
                              value: {dv: String}},
                          i: {key: {ip: String,
                                    iv: String},
                              value: {ii0: UInt64,
                                      ii1: UInt64}}}

  class Chest
    def initialize(@sophia : Env)
    end

    protected def oid : Oid
      uuid = UUID.v7
      b = uuid.bytes.to_slice
      {IO::ByteFormat::BigEndian.decode(UInt64, b[0..7]),
       IO::ByteFormat::BigEndian.decode(UInt64, b[8..15])}
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

    protected def add(tx : Env, i : Oid, p : String, o : String | Int64 | Float64 | Bool | Nil)
      oe = case o
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
      tx << {di0: i[0], di1: i[1], dp: p, dv: oe}
      tx << {ip: p, iv: oe, ii0: i[0], ii1: i[1]}
    end

    protected def add(tx : Env, i : Oid, p : String, o : H)
      o.each do |k, v|
        add tx, i, p.empty? ? k.to_s : "#{p}.#{k}", v
      end
    end

    protected def add(tx : Env, i : Oid, p : String, o : Array(A))
      o.each_with_index do |v, k|
        add tx, i, p.empty? ? k.to_s : "#{p}.#{k}", v
      end
    end

    protected def add(tx : Env, i : Oid, p : String, o : A)
      if os = o.as_s?
        add tx, i, p, os
      elsif oi = o.as_i64?
        add tx, i, p, oi
      elsif of = o.as_f?
        add tx, i, p, of
      elsif (ob = o.as_bool?) != nil
        add tx, i, p, ob.not_nil!
      elsif oh = o.as_h?
        add tx, i, p, oh
      elsif oa = o.as_a?
        add tx, i, p, oa
      else
        add tx, i, p, o.as_nil
      end
    end

    def <<(o : A)
      i = oid
      @sophia.transaction do |tx|
        add tx, i, "", o
      end
      i
    end

    def h2a(a : A) : A
      if ah = a.as_h?
        if ah.has_key? "0"
          return A.new AA.new(ah.size) { |i| h2a ah[i.to_s] }
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

    def []?(i : Oid, p : Array(String) = [] of String)
      flat = H.new
      @sophia.from({di0: i[0], di1: i[1], dp: p.join '.'}) do |d|
        break unless d[:di0] == i[0] && d[:di1] == i[1]
        flat[d[:dp]] = A.new decode d[:dv]
      end
      return nil unless flat.size > 0
      return flat[""] if flat.has_key? ""
      h2a A.new nest flat
    end

    def []?(i : Oid, *p : String)
      self[i, p]?
    end

    def delete(i : Oid, p : Array(String) = [] of String)
      @sophia.transaction do |tx|
        @sophia.from({di0: i[0], di1: i[1], dp: p.join '.'}) do |d|
          break unless d[:di0] == i[0] && d[:di1] == i[1]
          tx.delete({di0: d[:di0], di1: d[:di1], dp: d[:dp]})
          tx.delete({ip: d[:dp], iv: d[:dv]})
        end
      end
    end

    def delete(i : Oid, *p : String)
      self.delete i, p
    end
  end
end
