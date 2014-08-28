require 'spec_helper'

describe Workers::Filter do
  it 'pass through filter' do
    job = SideJob.queue('core', 'Workers::Filter')
    data = [{'abc' => 123, 'xyz' => 'foo'}, [1, 2, 3, "abc"]]
    data.each do |x|
      job.input(:in).write_json x
    end
    job.input(:filter).write '.'
    SideJob::Worker.drain_queue
    data.each do |x|
      expect(job.output(:out).read_json).to eq x
    end
  end

  it 'lookup filter number' do
    job = SideJob.queue('core', 'Workers::Filter')
    job.input(:in).write_json({"foo" => 42, "bar" => "hello"})
    job.input(:filter).write '.foo'
    SideJob::Worker.drain_queue
    expect(job.output(:out).read).to eq '42'
  end

  it 'lookup filter string' do
    job = SideJob.queue('core', 'Workers::Filter')
    job.input(:in).write_json({"foo" => 42, "bar" => "hello"})
    job.input(:filter).write '.bar'
    SideJob::Worker.drain_queue
    expect(job.output(:out).read).to eq '"hello"'
  end

  it 'string interpolation' do
    job = SideJob.queue('core', 'Workers::Filter')
    job.input(:in).write_json({"foo" => 42, "bar" => "hello"})
    job.input(:filter).write'"\(.bar) world: \(.foo+1)"'
    SideJob::Worker.drain_queue
    expect(job.output(:out).read).to eq '"hello world: 43"'
  end

  it 'length calculation' do
    job = SideJob.queue('core', 'Workers::Filter')
    job.input(:in).write_json({"foo" => [1, 2, 3]})
    job.input(:filter).write '.foo | length'
    SideJob::Worker.drain_queue
    expect(job.output(:out).read).to eq '3'
  end
end
