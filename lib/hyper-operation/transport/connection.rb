module Hyperloop
  module AutoCreate
    def needs_init?
      return false if Hyperloop.transport == :none
      return true if connection.respond_to?(:data_sources) && !connection.data_sources.include?(table_name)
      return true if !connection.respond_to?(:data_sources) && !connection.tables.include?(table_name)
      return false unless Hyperloop.on_server?
      return true if defined?(Rails::Server)
      return true unless Connection.root_path
      uri = URI("#{Connection.root_path}server_up")
      http = Net::HTTP.new(uri.host, uri.port)
      request = Net::HTTP::Get.new(uri.path)
      if uri.scheme == 'https'
        http.use_ssl = true
        http.verify_mode = OpenSSL::SSL::VERIFY_NONE
      end
      http.request(request) && return rescue true
    end

    def create_table(*args, &block)
      connection.create_table(table_name, *args, &block) if needs_init?
    end
  end

  module RedisRecord
    class Base
      class << self
        def client
          @client ||= Redis.new(url: 'redis://127.0.0.1:6379/hyperloop')
        end
      end
    end

    class HashItem < Base
      class << self
        def all
          Hash[client.hgetall(name).map { |k, v| [k, JSON.parse(v)] }]
        end

        def delete(key)
          client.hdel(name, key)
        end

        def exists?(key)
          client.hexists(name, key)
        end

        def rget(key)
          value = client.hget(name, key)
  
          if value.blank?
            nil
          else
            JSON.parse(value)
          end
        end
  
        def rset(key, value)
          value = value.to_json if value
  
          client.hset(name, key, value)
  
          value
        end
      end
    end

    class SetItem < Base
      class << self
        def create(opts = {})
          client.sadd(name, opts.to_json)
        end

        def delete(m)
          client.srem(name, m)
        end

        def all
          client.smembers(name).map { |m| JSON.parse(m) }
        end
      end
    end
  end

  class Connection < RedisRecord::HashItem
    class QueuedMessage
      class << self
        def create(connection, data)
          session = connection['session'] ? "#{connection['session']}:" : nil
          key = "connection:#{connection['channel']}:#{session}queued_messages"

          client.zadd(key, data)
        end

        def root_path=(_path)
          # find_or_create_by(connection_id: 0).update(data: path)
        end

        def root_path
          # find_or_create_by(connection_id: 0).data
        end
      end
    end

    class << self
      attr_accessor :transport

      def inactive
        all.select do |_channel, data|
          !data['session'] && data['refresh_at'] < Time.zone.now
        end.compact
      end

      def expired
        all.select do |_channel, data|
          data['expires_at'] && data['expires_at'] < Time.zone.now
        end.compact
      end

      def pending_for(channel_name)
        all.select do |channel, data|
          channel == channel_name && data['session']
        end.compact
      end

      def needs_refresh?
        all.select do |_channel, data|
          data['refresh_at'] && data['refresh_at'] < Time.zone.now
        end
      end

      def active
        start = Time.now
        if Hyperloop.on_server?
          expired.each { |channel| delete(channel) }
          refresh_connections if needs_refresh?
        end

        result = client.hkeys('Connection')
        puts "%%%%%% ACTIVE TOOK #{Time.now - start}"
        result
      end

      def open(channel, session = nil, root_path = nil)
        puts "!!!!!!!! OPENH "
        self.root_path = root_path

        existing = rget(channel)

        return existing if existing

        data = {}.tap do |hash|
          if session
            hash[:expires_at] = Time.now + transport.expire_new_connection_in
          elsif transport.refresh_channels_every != :never
            hash[:refresh_at] = Time.now + transport.refresh_channels_every
          end
        end

        channel += ":#{session}" if session

        rset(channel, data)
      end

      def send_to_channel(channel, data)
        puts "!!!!!!!! SECND TO SACHENL"
        pending_for(channel).each do |connection|
          QueuedMessage.create(connection: connection, data: data)
        end

        transport.send_data(channel, data) if exists?(channel) && !rget(channel)['session']
      end

      def read(session, root_path)
        puts "!!!!!!!!REDAD"
        self.root_path = root_path

        all.map do |channel, data|
          rget(channel) if data['session'] && data['session'] == session
        end.compact.each do |c|
          rset(channel, c.merge(expires_at: Time.now + transport.expire_polled_connection_in))
        end

        messages = []

        connections = all.select { |c| c['session'] == session }

        connections.each_key do |channel|
          session = session ? "#{session}:" : nil
          puts "ABOUT TO READ N DEL connection:#{channel}:#{session}queued_messages"
          client.zrange("connection:#{channel}:#{session}queued_messages", 0, -1).each do |m|
            messages << m
            client.zrem("connection:#{channel}:#{session}queued_messages", m)
          end
        end
        # QueuedMessage.all.map do |message|
        #   if message['session'] == session
        #     messages << data['data']
        #     QueuedMessage.delete(m)
        #   end
        # end

        messages
      end

      def connect_to_transport(channel, session, root_path)
        puts "!!!!!!!! ECONNTEC TO TRANS"
        self.root_path = root_path

        if (connection = rget(channel)) && connection['session'] == session
          session = session ? "#{session}:" : nil
          puts "ABOUT TO READ  connection:#{channel}:#{session}queued_messages"
          messages = client.zrange("connection:#{channel}:#{session}queued_messages", 0, -1)

          delete(channel)
        else
          messages = []
        end

        open(channel)

        messages
      end

      def disconnect(channel)
        delete(channel)
      end

      def root_path=(path)
        QueuedMessage.root_path = path if path
      end

      def root_path
        QueuedMessage.root_path
      rescue
        nil
      end

      def refresh_connections
        puts "!!!!!!!! reFERSH"
        refresh_started_at = Time.zone.now
        channels = transport.refresh_channels
        next_refresh = refresh_started_at + transport.refresh_channels_every

        channels.each do |channel|
          connection = rget(channel)
          rset(channel, connection.merge(refresh_at: next_refresh)) if connection
        end

        inactive.each { |channel| delete(channel) }
      end
    end
  end
end


    # class QueuedMessage < ActiveRecord::Base

    #   extend AutoCreate

    #   self.table_name = 'hyperloop_queued_messages'

    #   do_not_synchronize

    #   serialize :data

    #   belongs_to :hyperloop_connection,
    #              class_name: 'Hyperloop::Connection',
    #              foreign_key: 'connection_id'

    #   scope :for_session,
    #         ->(session) { joins(:hyperloop_connection).where('session = ?', session) }

    #   # For simplicity we use QueuedMessage with connection_id 0
    #   # to store the current path which is used by consoles to
    #   # communicate back to the server

    #   default_scope { where('connection_id IS NULL OR connection_id != 0') }

    #   def self.root_path=(path)
    #     unscoped.find_or_create_by(connection_id: 0).update(data: path)
    #   end

    #   def self.root_path
    #     unscoped.find_or_create_by(connection_id: 0).data
    #   end
    # end

#     extend AutoCreate

#     def self.build_tables
#       create_table(force: :cascade) do |t|
#         t.string   :channel
#         t.string   :session
#         t.datetime :created_at
#         t.datetime :expires_at
#         t.datetime :refresh_at
#       end
#       QueuedMessage.create_table(force: :cascade) do |t|
#         t.text    :data
#         t.integer :connection_id
#       end
#     end

#     do_not_synchronize

#     self.table_name = 'hyperloop_connections'

#     has_many :messages,
#              foreign_key: 'connection_id',
#              class_name: 'Hyperloop::Connection::QueuedMessage',
#              dependent: :destroy
#     scope :expired,
#           -> { where('expires_at IS NOT NULL AND expires_at < ?', Time.zone.now) }
#     scope :pending_for,
#           ->(channel) { where(channel: channel).where('session IS NOT NULL') }

#     scope :inactive,
#           -> { where('session IS NULL AND refresh_at < ?', Time.zone.now) }

#     def self.needs_refresh?
#       exists?(['refresh_at IS NOT NULL AND refresh_at < ?', Time.zone.now])
#     end

#     def transport
#       self.class.transport
#     end

#     before_create do
#       if session
#         self.expires_at = Time.now + transport.expire_new_connection_in
#       elsif transport.refresh_channels_every != :never
#         self.refresh_at = Time.now + transport.refresh_channels_every
#       end
#     end

#     class << self


#       def active
#         ractive

#         # if Hyperloop.on_server?
#         #   expired.delete_all
#         #   refresh_connections if needs_refresh?
#         # end
#         # all.pluck(:channel).uniq
#       end



#       def open(channel, session = nil, root_path = nil)
#         ropen(channel, session, root_path)

#         # self.root_path = root_path

#         # find_or_create_by(channel: channel, session: session)
#       end



#       def send_to_channel(channel, data)
#         rsend_to_channel(channel, data)

#         # pending_for(channel).each do |connection|
#         #   QueuedMessage.create(data: data, hyperloop_connection: connection)
#         # end
#         # transport.send_data(channel, data) if exists?(channel: channel, session: nil)
#       end



#       def read(session, root_path)
#         rread(session, root_path)

#         # self.root_path = root_path
#         # where(session: session)
#         #   .update_all(expires_at: Time.now + transport.expire_polled_connection_in)
#         # QueuedMessage.for_session(session).destroy_all.pluck(:data)
#       end



#       def connect_to_transport(channel, session, root_path)
#         rconnect_to_transport(channel, session, root_path)

#         # self.root_path = root_path
#         # if (connection = find_by(channel: channel, session: session))
#         #   messages = connection.messages.pluck(:data)
#         #   connection.destroy
#         # else
#         #   messages = []
#         # end
        
#         # open(channel)
#         # messages
#       end


#       def disconnect(channel)
#         rdisconnect(channel)

#         # find_by(channel: channel, session: nil).destroy
#       end

#       def refresh_connections
#         rrefresh_connections

#         # refresh_started_at = Time.zone.now
#         # channels = transport.refresh_channels
#         # next_refresh = refresh_started_at + transport.refresh_channels_every
#         # channels.each do |channel|
#         #   connection = find_by(channel: channel, session: nil)
#         #   connection.update(refresh_at: next_refresh) if connection
#         # end
#         # inactive.delete_all
#       end

#       def rget(key)
#         value = client.hget('Connection', key)

#         if value.blank?
#           nil
#         else
#           JSON.parse(value)
#         end
#       end

#       def rset(key, value)
#         value = value.to_json if value

#         client.hset('Connection', key, value)

#         value
#       end
#     end
#   end
# end
