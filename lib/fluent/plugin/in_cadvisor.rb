require 'rest-client'
require 'digest/sha1'
require 'time'
require 'docker'

class CadvisorInput < Fluent::Input
  class TimerWatcher < Coolio::TimerWatcher

    def initialize(interval, repeat, log, &callback)
      @callback = callback
      @log = log

      super(interval, repeat)
    end
    def on_timer
      @callback.call
    rescue
      @log.error $!.to_s
      @log.error_backtrace
    end
  end

  Fluent::Plugin.register_input('cadvisor', self)

  config_param :host, :string, :default => 'localhost'
  config_param :port, :string, :default => 8080
  config_param :api_version, :string, :default => '1.1'
  config_param :stats_interval, :time, :default => 10 # 10sec
  config_param :tag, :string, :default => "log.cadvisor"
  config_param :item_prefix, :string, :default => "container"
  config_param :docker_url, :string,  :default => 'unix:///var/run/docker.sock'

  def initialize
    super
    require 'socket'

    Docker.url = @docker_url
    @hostname = Socket.gethostname
    @dict     = {}
  end

  def configure(conf)
    super
  end

  def start
    @cadvisorEP ||= "http://#{@host}:#{@port}/api/v#{@api_version}"
    @machine    ||= get_spec

    @loop = Coolio::Loop.new
    tw = TimerWatcher.new(@stats_interval, true, @log, &method(:get_metrics))
    tw.attach(@loop)
    @thread = Thread.new(&method(:run))
  end

  def run
    @loop.run
  rescue
    log.error "unexpected error", :error=>$!.to_s
    log.error_backtrace
  end

  def get_interval (current, previous)
    cur  = Time.parse(current).to_f
    prev = Time.parse(previous).to_f

    # to nano seconds
    (cur - prev) * 1000000000
  end

  def get_spec
    response = RestClient.get(@cadvisorEP + "/machine")
    JSON.parse(response.body)
  end

  # Metrics collection methods
  def get_metrics
    Docker::Container.all.each do |obj|
      emit_container_info(obj)
    end
  end

  def add_prefix(prefix, item)
    if prefix.nil?
      return item
    end
    return "#{prefix}.#{item}"
  end

  # interval: 7 - 9sec
  def emit_container_info(obj)
    container_json = obj.json
    config = container_json['Config']

    id   = container_json['Id']
    name = config['Image']
    env  = config['Hostname'].split('--')[2] || '' # app--version--env

    tag_galaxy_jobid = nil
    tag_galaxy_toolname = nil
    tag_galaxy_splittedjobid = nil
    ## nil if key is missing
    if not config['Labels'].nil?
      tag_galaxy_jobid = config['Labels']['tag_galaxy_jobid']
      tag_galaxy_toolname = config['Labels']['tag_galaxy_toolname']
      tag_galaxy_splittedjobid = config['Labels']['tag_galaxy_splittedjobid']
    end

    response = RestClient.get(@cadvisorEP + "/containers/docker/" + id)
    res = JSON.parse(response.body)

    # Set max memory
    memory_limit = @machine['memory_capacity'] < res['spec']['memory']['limit'] ? @machine['memory_capacity'] : res['spec']['memory']['limit']

    latest_timestamp = @dict[id] ||= 0

    # Remove already sent elements
    res['stats'].reject! do | stats |
      Time.parse(stats['timestamp']).to_i <= latest_timestamp
    end

    # TODO: handle only first - last / 10 sec interval
    ## TODO: check length
    len = res['stats'].length
    if len <= 1
      @log.info "no new stats: #{id}"
      return
    end
    stats = res['stats'][0]
    nextstat = res['stats'][len - 1]
    @dict[id] = Time.parse(nextstat['timestamp']).to_i
    timestamp = Time.parse(stats['timestamp']).to_i

    # # Break on last element
    # # We need 2 elements to create the percentage, in this case the prev will be
    # # out of the array
    # if index == (res['stats'].count - 1)
    #   @dict[id] = timestamp
    #   break
    # end

    num_cores = stats['cpu']['usage']['per_cpu_usage'].count

    # CPU percentage variables
    #nextstat           = res['stats'][index + 1];
    raw_usage      = nextstat['cpu']['usage']['total'] - stats['cpu']['usage']['total']
    interval_in_ns = get_interval(nextstat['timestamp'], stats['timestamp'])
    interval_in_s = interval_in_ns/1000000000.0

    #@log.info "interval_in_ns: #{interval_in_ns} #{interval_in_s} #{timestamp} #{@dict[id]} #{len}"
    network_receive_bytes_sec = ((nextstat['network']['rx_bytes'] - stats['network']['rx_bytes'])/interval_in_s).round(2)
    network_send_bytes_sec = ((nextstat['network']['tx_bytes'] - stats['network']['tx_bytes'])/interval_in_s).round(2)
    pgfault_sec = ((nextstat['memory']['container_data']['pgfault'] - stats['memory']['container_data']['pgfault'] )/interval_in_s).round(2)
    pgmajfault_sec = ((nextstat['memory']['container_data']['pgmajfault'] - stats['memory']['container_data']['pgmajfault'] )/interval_in_s).round(2)
    # TODO: multiple disks

    ## e.g. cadvisor container has no io stat...
    disk_read_bytes_sec = 0
    disk_write_bytes_sec = 0

    if nextstat['diskio'].has_key?('io_service_bytes')
      disk_read_bytes_sec = ((nextstat['diskio']['io_service_bytes'][0]['stats']['Read']-stats['diskio']['io_service_bytes'][0]['stats']['Read'])/interval_in_s).round(2)
      disk_write_bytes_sec = ((nextstat['diskio']['io_service_bytes'][0]['stats']['Write']-stats['diskio']['io_service_bytes'][0]['stats']['Write'])/interval_in_s).round(2)
    end

    record = {
      #'id'                 => Digest::SHA1.hexdigest("#{name}#{id}#{timestamp.to_s}"),
      'container_id'       => id,
      'image'              => name,
      'container_alias'    => res['aliases'][0],
      #
      #'environment'        => env,
      add_prefix(item_prefix, 'memory_usage_mb')     => (stats['memory']['usage']/(1024*1024)).round(2),
      #'memory_limit'       => memory_limit,
      #'cpu_usage'          => raw_usage,
      #'cpu_usage_pct'      => (((raw_usage / interval_in_ns ) / num_cores ) * 100).round(2),
      add_prefix(item_prefix, 'cpu_usage') => ((raw_usage/interval_in_ns)*100).round(2),
      add_prefix(item_prefix, 'cpu_num_cores')      => num_cores,
      add_prefix(item_prefix, 'network_receive_bytes_sec') => network_receive_bytes_sec,
      add_prefix(item_prefix,'network_send_bytes_sec') => network_send_bytes_sec,

      # swap (pgfault)
      add_prefix(item_prefix,'page_fault_sec') => pgfault_sec,
      add_prefix(item_prefix,'page_major_fault_sec') => pgmajfault_sec,

      # disk
      add_prefix(item_prefix,'disk_read_bytes_sec') => disk_read_bytes_sec,
      add_prefix(item_prefix,'disk_write_bytes_sec') => disk_write_bytes_sec,
    }

    record['tag_galaxy_jobid'] = tag_galaxy_jobid unless tag_galaxy_jobid.nil?
    record['tag_galaxy_toolname'] = tag_galaxy_toolname unless tag_galaxy_toolname.nil?
    record['tag_galaxy_splittedjobid'] = tag_galaxy_splittedjobid unless tag_galaxy_splittedjobid.nil?

    Fluent::Engine.emit(tag, timestamp, record)
  end

  def shutdown
    @loop.stop
    @thread.join
  end
end
