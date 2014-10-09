require 'spec_helper'

describe Workers::ArrayGenerator do
  before do
    @job = SideJob.queue('core', 'Workers::ArrayGenerator')
  end

  it 'suspends on no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
  end

  it 'completes on config but no input' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'suspends when missing some inputs' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    @job.input(:in1).write({key1: 'val1'})
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
  end

  it 'concatenates objects by default' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    @job.input(:in1).write [1,2], [3,4]
    @job.input(:in2).write [true, false], [nil, {}]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq([1, 2, true, false])
    expect(@job.output(:out).read).to eq([3, 4, nil, {}])
  end

  it 'raises error if port is not specified in config' do
    @job.input(:config).write [{},]
    expect { SideJob::Worker.drain_queue }.to raise_error
  end

  it 'suspends if missing some inputs after some concatenating' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    @job.input(:in1).write [1,2], [3,4]
    @job.input(:in2).write [true, false]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq([1, 2, true, false])
  end

  it 'saves and resumes with partially read inputs' do
    @job.input(:config).write [{port: 'in1'}, {port: 'in2'}]
    @job.input(:in1).write [1,2]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    @job.input(:in2).write [true, false]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq([1, 2, true, false])
  end

  it 'specify a port should use most recent data' do
    @job.input(:config).write [{port: 'in1', use_recent: true}, {port: 'in2'}]
    @job.input(:in1).write [1,2], [3,4]
    @job.input(:in2).write [true, false], [nil, {}]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq [3, 4, true, false]
    expect(@job.output(:out).read).to eq [3, 4, nil, {}]
  end

  it 'can convert from an object' do
    @job.input(:config).write [{port: 'in1', use_recent: true, from_object: true}, {port: 'in2'}]
    @job.input(:in1).write({key1: 'val1', key2: 'val2'})
    @job.input(:in2).write [1,2], [3,4]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq([['key1', 'val1'], ['key2', 'val2'], 1, 2])
    expect(@job.output(:out).read).to eq([['key1', 'val1'], ['key2', 'val2'], 3, 4])
  end

  it 'can convert a single value into an array' do
    @job.input(:config).write [{port: 'in1', collect: 1}, {port: 'in2'}]
    @job.input(:in1).write 1
    @job.input(:in2).write [2,3]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq([1, 2, 3])
  end

  it 'use_recent option is incompatible with collect' do
    @job.input(:config).write [{port: 'in1', use_recent: true, collect: 1}, {port: 'in2'}]
    @job.input(:in1).write 1, true
    @job.input(:in2).write [2,3]
    expect { SideJob::Worker.drain_queue }.to raise_error
  end

  it 'can be used to group an arbitrary number of packets into an array' do
    @job.input(:config).write [{port: 'in1', collect: 5}]
    @job.input(:in1).write 1, 2, 3
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    @job.input(:in1).write 4, 5
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq([1, 2, 3, 4, 5])
  end

  it 'can repeat array' do
    @job.input(:config).write [{port: 'in1', repeat: 2}, {port: 'in2'}]
    @job.input(:in1).write [1, 2]
    @job.input(:in2).write [3]
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq([1, 2, 1, 2, 3])
  end

  it 'can repeat single value' do
    @job.input(:config).write [{port: 'in1', repeat: 3, collect: 1}]
    @job.input(:in1).write 'a'
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq(['a', 'a', 'a'])
  end

  it 'can repeat with collect' do
    @job.input(:config).write [{port: 'in1', repeat: 2, collect: 3}]
    @job.input(:in1).write 1, 2, 3, 4
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq([1, 2, 3, 1, 2, 3])
  end
end
