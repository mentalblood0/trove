require "json"
require "yaml"
require "uuid"
require "compress/gzip"

require "xxhash128"
require "dream"
require "lawn/Database"

module Trove
  alias A = JSON::Any
  alias H = Hash(String, A)
  alias AA = Array(A)
  alias I = String | Int64 | Float64 | Bool | Nil

  OBJECT_ID_AND_PATH_TO_VALUE = 0_u8

  struct ObjectId
    getter value : Bytes

    def initialize(@value)
    end

    def self.random
      uuid = UUID.v7.bytes.to_slice
      result_value = Bytes.new 16
      IO::ByteFormat::BigEndian.encode uuid[0], result_value[0..7]
      IO::ByteFormat::BigEndian.encode uuid[1], result_value[8..15]
      self.new result_value
    end

    def self.from_string(s : String)
      from_bytes s.hexbytes
    end

    def string
      value.hexstring
    end

    def <=>(other : ObjectId)
      @value <=> other.value
    end
  end

  struct Digest
    getter value : Bytes

    def initialize(@value)
    end

    def self.of_data(data : Bytes)
      result_struct = LibXxhash.xxhash128 data, data.size, 0
      result_value = Bytes.new 16
      IO::ByteFormat::BigEndian.encode result_struct.high64, result_value[0..7]
      IO::ByteFormat::BigEndian.encode result_struct.low64, result_value[8..15]
      self.new result_value
    end

    def self.of_path_and_encoded_value(path_bytes : Bytes, encoded_value : Bytes)
      data = Bytes.new path_bytes.size + 1 + encoded_value.size
      path_bytes.to_unsafe.copy_to data.to_unsafe, data.bytesize
      encoded_value.copy_to data.to_unsafe + path_bytes.size + 1, encoded_value.size
      self.of_data data
    end
  end

  struct PartitionedPath
    getter base : Bytes
    getter index : Int32?

    def initialize(@base, @index)
    end

    def self.from_path(path : String)
      partitioned_path = path.rpartition '.'
      self.new(base: partitioned_path[0].to_slice, index: partitioned_path[2].to_i32) rescue self.new(base: path.to_slice, index: nil)
    end
  end

  class Transaction
    getter database_transaction : Lawn::Transaction
    getter index_transaction : Dream::Transaction

    def initialize(@database_transaction, @index_transaction)
    end

    macro yield_object
      if flat.size == 0
        yield({object_id, A.new nil})
      elsif flat.has_key? ""
        yield({object_id, flat[""]})
      else
        yield({object_id, to_json_object A.new nest flat})
      end
    end

    def objects(&)
      object_id : ObjectId? = nil
      flattened_object = H.new
      @database_transaction.cursor(OBJECT_ID_AND_PATH_TO_VALUE).each_next do |current_object_id_and_path, current_value|
        current_object_id = ObjectId.new current_object_id_and_path[..15]
        unless current_object_id == object_id
          if object_id
            yield_object
            flattened_object.clear
          end
          object_id = i
        end
        current_path = current_object_id_and_path[16..]
        flattened_object[current_path] = A.new decode current_value
      end
      if object_id
        yield_object
      end
    end

    def objects
      result = [] of {ObjectId, A}
      objects { |object| result << object }
      result
    end

    record DumpEntry, object_id : String, object : A

    def dump(stream : IO)
      Compress::Gzip::Writer.open(stream, Compress::Deflate::BEST_COMPRESSION) do |compressor|
        objects do |object_id, object|
          compressor.puts DumpEntry.new(object_id, object).to_json
        end
      end
    end

    def load(stream : IO)
      Compress::Gzip::Reader.open(stream) do |decompressor|
        decompressor.each_line do |line|
          dump_entry = DumpEntry.from_json line.chomp
          object_id = ObjectId.from_string dump_entry.object_id
          set object_id, "", dump_entry.object
        end
      end
    end

    ENCODED_TYPE_STRING     = 0_u8
    ENCODED_TYPE_INT8       = 1_u8
    ENCODED_TYPE_INT16      = 2_u8
    ENCODED_TYPE_INT32      = 3_u8
    ENCODED_TYPE_INT64      = 4_u8
    ENCODED_TYPE_FLOAT32    = 5_u8
    ENCODED_TYPE_FLOAT64    = 6_u8
    ENCODED_TYPE_BOOL_TRUE  = 7_u8
    ENCODED_TYPE_BOOL_FALSE = 8_u8

    protected def encode(value : I) : Bytes
      case value
      when String
        result = Bytes.new 1 + value.bytesize
        result[0] = ENCODED_TYPE_STRING
        value.to_unsafe.copy_to result.to_unsafe + 1, value.bytesize
        result
      when Int64
        if value >= Int8::MIN && value <= Int8::MAX
          result = Bytes.new 1 + 1
          result[0] = ENCODED_TYPE_INT8
          IO::ByteFormat::LittleEndian.encode value.to_i8!, result[1..]
          result
        elsif value >= Int16::MIN && value <= Int16::MAX
          result = Bytes.new 1 + 2
          result[0] = ENCODED_TYPE_INT16
          IO::ByteFormat::LittleEndian.encode value.to_i16!, result[1..]
          result
        elsif value >= Int32::MIN && value <= Int32::MAX
          result = Bytes.new 1 + 4
          result[0] = ENCODED_TYPE_INT32
          IO::ByteFormat::LittleEndian.encode value.to_i32!, result[1..]
          result
        else
          result = Bytes.new 1 + 8
          result[0] = ENCODED_TYPE_INT64
          IO::ByteFormat::LittleEndian.encode value, result[1..]
          result
        end
      when Float64
        if value.finite? && value == (value_as_float32 = value.to_f32).to_f64
          result = Bytes.new 1 + 4
          result[0] = ENCODED_TYPE_FLOAT32
          IO::ByteFormat::LittleEndian.encode value_as_float32.not_nil!, result[1..]
          result
        else
          result = Bytes.new 1 + 8
          result[0] = ENCODED_TYPE_FLOAT64
          IO::ByteFormat::LittleEndian.encode value, result[1..]
          result
        end
      when true  then Bytes.new 1, ENCODED_TYPE_BOOL_TRUE
      when false then Bytes.new 1, ENCODED_TYPE_BOOL_FALSE
      when nil   then "".to_slice
      else            raise "Can not encode #{value}"
      end
    end

    protected def decode(encoded : Bytes) : I
      return nil if encoded.empty?
      case encoded[0]
      when ENCODED_TYPE_STRING     then String.new encoded[1..]
      when ENCODED_TYPE_INT8       then IO::ByteFormat::LittleEndian.decode(Int8, encoded[1..]).to_i64!
      when ENCODED_TYPE_INT16      then IO::ByteFormat::LittleEndian.decode(Int16, encoded[1..]).to_i64!
      when ENCODED_TYPE_INT32      then IO::ByteFormat::LittleEndian.decode(Int32, encoded[1..]).to_i64!
      when ENCODED_TYPE_INT64      then IO::ByteFormat::LittleEndian.decode(Int64, encoded[1..])
      when ENCODED_TYPE_FLOAT32    then IO::ByteFormat::LittleEndian.decode(Float32, encoded[1..]).to_f64!
      when ENCODED_TYPE_FLOAT64    then IO::ByteFormat::LittleEndian.decode(Float64, encoded[1..])
      when ENCODED_TYPE_BOOL_TRUE  then true
      when ENCODED_TYPE_BOOL_FALSE then false
      end
    end

    class IndexBatch
      getter object_id : ObjectId
      getter transaction : Transaction
      getter type : Symbol

      getter digests = [] of Bytes
      getter array_digests = {} of Int32 => Array(Bytes)

      def initialize(@object_id, @transaction, @type)
      end

      def add(path : String, encoded_value : Bytes)
        partitioned_path = PartitionedPath.from_path path
        if path_index = partitioned_path.index
          @digests << Digest.of_path_and_encoded_value(partitioned_path.base, encoded_value).value
          @array_digests[path_index] = [] of Bytes unless @array_digests.has_key? path_index
          @array_digests[path_index] << Digest.of_path_and_encoded_value(partitioned_path.base, @object_id.value + encoded_value).value
        else
          @digests << Digest.of_path_and_encoded_value(path.to_slice, encoded_value).value
        end
        self
      end

      def self.path_index_to_bytes(path_index : Int32)
        result = Bytes.new 4
        IO::ByteFormat::BigEndian.encode path_index, result
        result
      end

      def self.path_index_from_bytes(bytes : Bytes)
        IO::ByteFormat::BigEndian.decode Int32, bytes
      end

      def flush
        case @type
        when :add
          @transaction.index_transaction.add @object_id.value, @digests
          @array_digests.each { |path_index, path_index_paths_digests| @transaction.index_transaction.add(IndexBatch.path_index_to_bytes(path_index), path_index_paths_digests) }
        when :delete
          @transaction.index_transaction.delete @object_id.value, @digests
          @array_digests.each { |path_index, path_index_paths_digests| @transaction.index_transaction.delete(IndexBatch.path_index_to_bytes(path_index), path_index_paths_digests) }
        else raise Exception.new "Unsupported index batch type: #{@type}"
        end
        self
      end
    end

    def where(present_pathvalues : Array({String, I}), absent_pathvalues : Array({String, I}) = [] of {String, I}, start_after_object : ObjectId? = nil, &)
      @index_transaction.find(
        present_pathvalues.map { |path, value| Trove.digest pad(path), encode value },
        absent_pathvalues.map { |path, value| Trove.digest pad(path), encode value },
        start_after_object ? start_after_object.value : nil) { |dream_object_id| yield ObjectId.new dream_object_id.value }
    end

    def where(present_pathvalues : Array({String, I}), absent_pathvalues : Array({String, I}) = [] of {String, I}, limit : Int32 = Int32::MAX, start_after_object : ObjectId? = nil, &)
      result = [] of ObjectId
      where(present_pathvalues, absent_pathvalues, limit, start_after_object) do |object_id|
        break if result.size >= limit
        result << object_id
      end
      result
    end

    def index_of(object_id : ObjectId, path : String, value : I) : UInt32?
      padded_path = pad path
      partitioned_path = Trove.partition padded_path
      @index_transaction.find([Digest.of_path_and_encoded_value partitioned_path.base, object_id.value + encode value]) { |path_index_bytes| return IndexBatch.path_index_from_bytes path_index_bytes }
    end

    protected def set(object_id : ObjectId, path : String, value : A::Type, index_batch : IndexBatch? = nil)
      case value
      when H
        value.each do |key, internal_value|
          escaped_key = key.gsub ".", "\\."
          set object_id, path.empty? ? escaped_key : "#{path}.#{escaped_key}", internal_value.raw, index_batch
        end
        index_batch
      when AA
        array_index = 0_u32
        unique_internal_values = Set(String | Int64 | Float64 | Bool | Nil).new
        value.each do |internal_value_variant|
          internal_value = internal_value_variant.raw
          case internal_value
          when String, Int64, Float64, Bool, Nil
            next if unique_internal_values.includes? internal_value
            unique_internal_values << internal_value
          end
          key = array_index.to_s.rjust 10, '0'
          set object_id, path.empty? ? key : "#{path}.#{key}", internal_value, index_batch
          array_index += 1
        end
        index_batch
      else
        encoded_value = encode value
        index_batch.add path, encoded_value if index_batch
        @database_transaction.set OBJECT_ID_AND_PATH_TO_VALUE, object_id.value + path.to_slice, encoded_value
        index_batch
      end
    end

    def set(object_id : ObjectId, path : String, value : A)
      delete object_id, path
      set(object_id, path, value.raw, IndexBatch.new object_id, self, :add).flush
    end

    protected def deletei(object_id : ObjectId, path : String)
      IndexBatch
        .new(object_id, self, :delete)
        .add(path, (@database_transaction.get(OBJECT_ID_AND_PATH_TO_VALUE, object_id.value + path.to_slice).not_nil! rescue return))
        .flush
    end

    def set!(object_id : ObjectId, path : String, value : A)
      padded_path = pad path
      case raw_value = value.raw
      when Bool, Float64, Int64, String, Nil
        partitioned_path = PartitionedPath.from_path path
        return if (path_index = partitioned_path.index) && @index_transaction.has_tag? object_id.value, Digest.of_path_and_encoded_value partitioned_path.base, encode raw_value
      end
      deletei object_id, path if has_key! object_id, path
      set(object_id, path, raw_value, IndexBatch.new object_id, self, :add).flush
    end

    def <<(value : A)
      object_id = ObjectId.random
      set object_id, "", value
      object_id
    end

    protected def to_json_object(value : A) : A
      if value_as_hashmap = value.as_h?
        if value_as_hashmap.keys.all? { |key| key.to_u32? }
          return A.new value_as_hashmap.keys.sort_by { |key| key.to_u32 }.map { |key| to_json_object value_as_hashmap[key] }
        else
          value_as_hashmap.each { |key, internal_value| value_as_hashmap[key] = to_json_object internal_value }
        end
      end
      value
    end

    protected def nest(hashmap : H)
      result = H.new
      hashmap.each do |path, value|
        splitted_path = path.split /(?<!\\)\./
        current = result

        splitted_path.each_with_index do |escaped_segment, segment_index|
          key = escaped_segment.gsub "\\.", "."
          if segment_index == splitted_path.size - 1
            current[key] = value
          else
            current[key] ||= A.new H.new
            current = current[key].as_h
          end
        end
      end
      result
    end

    protected def pad(path : String)
      path.gsub(/\b\d{1,9}\b/) { |segment| segment.rjust 10, '0' }
    end

    protected def unpad(path : String)
      path.gsub(/\b(\d{10})\b/) { (result = $1.lstrip '0').empty? ? "0" : result }
    end

    def first(object_id : ObjectId, path : String = "") : {String, A}?
      padded_path = pad p
      @database_transaction.cursor(OBJECT_ID_AND_PATH_TO_VALUE, from: object_id.value + "#{padded_path}.".to_slice).each_next do |current_object_id_and_path, current_value|
        current_object_id = ObjectId.new current_object_id_and_path[..15]
        current_path = current_object_id_and_path[16..]
        break unless current_object_id == object_id.value && String.new(current_path).starts_with? padded_path
        result_path = unpad current_path[..(padded_path.empty? ? padded_path.size - 1 : padded_path.size) + 10]
      end
    end

    def last(object_id : ObjectId, path : String = "") : {String, A}?
      padded_path = pad path
      @database_transaction.cursor(OBJECT_ID_AND_PATH_TO_VALUE, from: object_id.value + padded_path.empty? ? "9" : "#{padded_path}.9", direction: :backward).each_next do |current_object_id_and_path, current_value|
        current_object_id = ObjectId.new current_object_id_and_path[..15]
        current_path = current_object_id_and_path[16..]
        break unless current_object_id == object_id.value && String.new(current_path).starts_with? padded_path
        result_path = unpad current_path[..(padded_path.empty? ? padded_path.size - 1 : padded_path.size) + 10]
        return {result_path, get(object_id, result_path).not_nil!}
      end
    end

    def push(object_id : ObjectId, path : String, values : AA) : UInt32
      padded_path = pad path
      last_path = ((last object_id, padded_path).not_nil![0] rescue "#{padded_path}.")
      partitioned_last_path = last_path.rpartition '.'
      base = partitioned_last_path.base
      first_index = (partitioned_last_path.index.to_u32 + 1 rescue 0_u32)
      unique_values = Set(String | Int64 | Float64 | Bool | Nil).new
      values.each_with_index do |value, value_local_index|
        new_path = "#{base.empty? ? "" : "#{base}."}#{(first_index + value_local_index).to_s.rjust 10, '0'}"
        case raw_value = value.raw
        when Bool, Float64, Int64, String, Nil
          next if unique_values.includes? raw_value
          unique_values << raw_value
          partitioned_new_path = PartitionedPath.from_path new_path
          next if @index_transaction.has_tag? object_id.value, Digest.of_path_and_encoded_value partitioned_new_path.base, encode raw_value
        end
        set(object_id, new_path, raw_value, IndexBatch.new object_id, self, :add).flush
      end
      first_index
    end

    def has_key?(object_id : ObjectId, path : String = "") : Bool
      padded_path = pad path
      @database_transaction.cursor(OBJECT_ID_AND_PATH_TO_VALUE, from: object_id.value + padded_path.to_slice).each_next do |current_object_id_and_path, current_value|
        current_object_id = ObjectId.new current_object_id_and_path[..15]
        current_path = current_object_id_and_path[16..]
        return current_object_id == object_id && String.new(current_path).starts_with? padded_path
        return d[:di0] == i.value[0] && d[:di1] == i.value[1] && d[:dp].starts_with? p
      end
      false
    end

    def has_key!(object_id : ObjectId, path : String = "") : Bool
      padded_path = pad path
      @database_transaction.get(OBJECT_ID_AND_PATH_TO_VALUE, from: object_id.value + padded_path.to_slice) != nil
    end

    def get(object_id : ObjectId, path : String = "")
      padded_path = pad path
      flat = H.new
      @database_transaction.cursor(OBJECT_ID_AND_PATH_TO_VALUE, from: object_id.value + padded_path.to_slice).each_next do |current_object_id_and_path, current_value|
        current_object_id = ObjectId.new current_object_id_and_path[..15]
        current_path = String.new(current_object_id_and_path[16..])
        break unless current_object_id == object_id && current_path.starts_with? padded_path
        flat[unpad current_path.lchop(padded_path).lchop('.')] = A.new decode current_value
      end
      return nil if flat.size == 0
      return flat[""] if flat.has_key? ""
      to_json_object A.new nest flat
    end

    def get!(object_id : ObjectId, path : String)
      padded_path = pad path
      decode @database_transaction.get(OBJECT_ID_AND_PATH_TO_VALUE, object_id.value + padded_path.to_slice).not_nil! rescue nil
    end

    protected def delete(object_id : ObjectId, path : String, encoded_value : Bytes, index_batch : IndexBatch)
      padded_path = pad path
      @database_transaction.delete OBJECT_ID_AND_PATH_TO_VALUE, object_id.value + padded_path.to_slice
      index_batch.add padded_path, encoded_value
      index_batch
    end

    def delete(object_id : ObjectId, path : String = "")
      padded_path = pad path
      return unless has_key? object_id, padded_path
      index_batch = IndexBatch.new object_id, self, :delete
      @database_transaction.cursor(OBJECT_ID_AND_PATH_TO_VALUE, from: object_id.value + padded_path.to_slice).each_next do |current_object_id_and_path, current_value|
        current_object_id = ObjectId.new current_object_id_and_path[..15]
        current_path = current_object_id_and_path[16..]
        break unless current_object_id == object_id && String.new(current_path).starts_with? padded_path
        delete object_id, String.new(current_path), current_value, index_batch
      end
      index_batch.flush
    end

    def delete!(object_id : ObjectId, path : String = "")
      padded_path = pad path
      return unless has_key! object_id, padded_path
      delete(object_id, padded_path, (@database_transaction.get OBJECT_ID_AND_PATH_TO_VALUE, object_id.value + padded_path.to_slice rescue return), IndexBatch.new object_id, self, :delete).flush
    end

    def commit
      @index_transaction.commit
      @database_transaction.commit
    end
  end

  class Chest
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter database : Lawn::Database
    getter index : Dream::Index

    def initialize(@database, @index)
    end

    def transaction
      Transaction.new @database.transaction, @index.transaction
    end

    def transaction(&)
      result = transaction
      yield result
      result.commit
    end
  end
end
