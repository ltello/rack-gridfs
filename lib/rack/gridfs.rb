require 'timeout'
require 'mongo'

module Rack
  class GridFSConnectionError < StandardError ; end

  class GridFS
    VERSION = "0.2.0"

    def initialize(app, options = {})
      options = {
        :hostname => 'localhost',
        :port     => Mongo::Connection::DEFAULT_PORT,
        :prefix   => 'gridfs',
        :lookup   => :id
      }.merge(options)
      
      # @default_avatar_file = nil

      @app        = app
      @prefix     = options[:prefix].gsub(/^\//, '')
      @lookup     = options[:lookup]
      @db         = nil

      @hostname, @port, @database, @username, @password = 
        options.values_at(:hostname, :port, :database, :username, :password)

      connect!
    end

    def call(env)
      request = Rack::Request.new(env)
      if request.path_info =~ /^\/#{@prefix}\/(.+)$/
        gridfs_request($1)
      else
        @app.call(env)
      end
    end

    private
      def connect!
        Timeout::timeout(5) do
          @db = Mongo::Connection.new(@hostname, @port).db(@database)
          @db.authenticate(@username, @password) if @username
        end
      rescue Exception => e
        raise Rack::GridFSConnectionError, "Unable to connect to the MongoDB server (#{e.to_s})"
      end

      def gridfs_request(identifier)
        puts "RACK-GRIDFS: IMAGEN SOLICITADA: #{identifier}"
        begin
          file, code, id = gridfs_request_inner(identifier)
          puts "RACK-GRIDFS: IMAGEN SERVIDA: #{id}"
          [code, {'Content-Type' => file.content_type}, file]
        rescue Mongo::GridFileNotFound, BSON::InvalidObjectId
          [404, {'Content-Type' => 'text/plain'}, ['File not found.']]
        end        
      end
      
      def gridfs_request_inner(identifier)
        begin
          file = [find_file(identifier), 200, identifier]
        rescue Mongo::GridFileNotFound, BSON::InvalidObjectId
            identifier =~ /image.jpg/  ? [find_file((id = default_imagejpg_identifier(identifier)), :path), 200, id] : raise
        end
      end

      def find_file(identifier, lookup=nil)
        case (lookup || @lookup).to_sym
        when :id   then Mongo::Grid.new(@db).get(BSON::ObjectId.from_string(identifier))
        when :path then Mongo::GridFileSystem.new(@db).open(identifier, "r")
        end
      end
      
      def default_imagejpg_identifier(identifier)
        identifier.gsub!(/image\.jpg\-\d*\z/,'image.jpg')
        return identifier if identifier.gsub!(/\/user\/[\w\d]+\//, '/user/default/')
        return identifier if identifier.gsub!(/\/club\/[\w\d]+\/team\/[\w\d]+\//, '/team/default/')
        return identifier if identifier.gsub!(/\/club\/[\w\d]+\//, '/club/default/')
        raise Mongo::GridFileNotFound
      end
      

  end # GridFS class
end # Rack module