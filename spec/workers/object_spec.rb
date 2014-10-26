require 'spec_helper'

describe Workers::Object do
  before do
    @job = SideJob.queue('core', 'Workers::Object', inports: {key1: {}, key2: {}})
  end

  it 'completes on no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'suspends when missing some inputs' do
    @job.input(:key1).write([1,2])
    @job.input(:key2)
    @job.run_inline
    expect(@job.status).to eq 'suspended'
  end

  it 'saves and resumes with partially read inputs' do
    @job.input(:key1).write([1,2])
    @job.input(:key2)
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    @job.input(:key2).write true
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq({'key1' => [1,2], 'key2' => true})
  end
end
