require 'sidejob'

Dir[File.dirname(__FILE__) + '/workers/*.rb'].each {|file| require file}
