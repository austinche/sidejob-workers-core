require 'bundler/setup'

require 'rspec/core'
require 'sidejob'
require 'sidejob/testing'
require 'webmock/rspec'
require_relative '../lib/workers-core'

SideJob.redis = {url: 'redis://localhost:6379/6'}

RSpec.configure do |config|
  config.order = 'random'
  config.before(:each) do
    SideJob.redis.flushdb
  end
end
