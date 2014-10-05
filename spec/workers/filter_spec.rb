require 'spec_helper'

describe Workers::Filter do
  before do
    @job = SideJob.queue('core', 'Workers::Filter')
  end
  
  it 'pass through filter with a variety of data types' do
    data = [{'abc' => 123, 'xyz' => 'foo'}, [1, 2, 3, "abc"], 123, 'string', true, false, nil]
    @job.input(:in).write *data
    expect(@job.input(:in).size).to eq data.length
    @job.input(:filter).write '.'
    SideJob::Worker.drain_queue
    data.each do |x|
      expect(@job.output(:out).read).to eq x
    end
    expect(@job.status).to eq 'completed'
  end

  it 'lookup filter number' do
    @job.input(:in).write({"foo" => 42, "bar" => "hello"})
    @job.input(:filter).write '.foo'
    SideJob::Worker.drain_queue
    expect(@job.output(:out).read).to eq 42
  end

  it 'lookup filter string' do
    @job.input(:in).write({"foo" => 42, "bar" => "hello"})
    @job.input(:filter).write '.bar'
    SideJob::Worker.drain_queue
    expect(@job.output(:out).read).to eq "hello"
  end

  it 'string interpolation' do
    @job.input(:in).write({"foo" => 42, "bar" => "hello"})
    @job.input(:filter).write'"\(.bar) world: \(.foo+1)"'
    SideJob::Worker.drain_queue
    expect(@job.output(:out).read).to eq 'hello world: 43'
  end

  it 'length calculation' do
    @job.input(:in).write({"foo" => [1, 2, 3]})
    @job.input(:filter).write '.foo | length'
    SideJob::Worker.drain_queue
    expect(@job.output(:out).read).to eq 3
  end

  it 'raises error on invalid filter' do
    @job.input(:in).write({"foo" => [1, 2, 3]})
    @job.input(:filter).write 'syntax error'
    expect { SideJob::Worker.drain_queue }.to raise_error
  end

  it 'can pass variables' do
    @job.input(:vars).write({foo: [1, 2], bar: 3})
    @job.input(:filter).write '(($foo | fromjson) | add) + ($bar | tonumber) + (. | tonumber)'
    @job.input(:in).write 4
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq 10
  end

  it 'can wait for vars' do
    @job.input(:vars).write true
    @job.input(:filter).write '(($foo | fromjson) | add) + ($bar | tonumber) + (. | tonumber)'
    @job.input(:in).write 4
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    @job.input(:vars).write({foo: [1, 2], bar: 3})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq 10
  end
end
