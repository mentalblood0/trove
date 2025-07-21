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
      when 'u'
        s[1..].gsub(/\\u([0-9a-fA-F]{4})/) do |m|
          c = $1.to_i(16)
          c.chr
        end
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

    protected def payload(i : Oid, p : Array(String), o : String | Int64 | Float64 | Bool | Nil)
      pj = p.join '.'
      oe = case o
           when String
             if o.ascii_only?
               "s#{o}"
             else
               os = o.gsub(/./) do |s|
                 if s[0].ord <= 127
                   s
                 else
                   "\\u#{s[0].ord.to_s(16).rjust(4, '0')}"
                 end
               end
               "u#{os}"
             end
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
      [{di0: i[0], di1: i[1], dp: pj, dv: oe}, {ip: pj, iv: oe, ii0: i[0], ii1: i[1]}]
    end

    protected def payload(i : Oid, p : Array(String), o : H)
      r = [] of Record
      o.each do |k, v|
        r += payload i, (p + [k]), v
      end
      r
    end

    protected def payload(i : Oid, p : Array(String), o : Array(A))
      r = [] of Record
      o.each_with_index do |v, k|
        r += payload i, (p + [k.to_s]), v
      end
      r
    end

    protected def payload(i : Oid, p : Array(String), o : A)
      if os = o.as_s?
        payload i, p, os
      elsif oi = o.as_i64?
        payload i, p, oi
      elsif of = o.as_f?
        payload i, p, of
      elsif (ob = o.as_bool?) != nil
        payload i, p, ob.not_nil!
      elsif oh = o.as_h?
        payload i, p, oh
      elsif oa = o.as_a?
        payload i, p, oa
      else
        payload i, p, o.as_nil
      end
    end

    def <<(o : A)
      i = oid
      @sophia << payload i, [] of String, o
      i
    end

    def h2a(a : A) : A
      if ah = a.as_h?
        if ah.has_key? "0"
          return A.new AA.new(ah.size) { |i| h2a ah[i.to_s] }
        else
          ah.each { |k, v| ah[k] = h2a v }
        end
      elsif aa = a.as_a?
        (0..aa.size - 1).each { |i| aa[i] = h2a aa[i] }
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
  end
end
