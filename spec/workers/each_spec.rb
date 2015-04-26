require 'spec_helper'

describe Workers::Each do
  before do
    @job = SideJob.queue('core', 'Workers::Each')
  end

  it 'outputs array elements' do
    @job.input(:in).write((0..4).to_a)
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).entries).to eq (0..4).to_a
  end

  it 'outputs an object as key value pairs' do
    @job.input(:in).write({key1: 1, key2: 2, key3: 3})
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 3
    expect(@job.output(:out).entries).to match_array([['key1', 1], ['key2', 2], ['key3', 3]])
  end
end
