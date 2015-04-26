require 'spec_helper'

describe Workers::Connect do
  before do
    @job = SideJob.queue('core', 'Workers::Connect')
  end

  it 'completes when no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'connects multiple packets in order' do
    data = {'test' => 123}
    @job.input(:in).write(data)
    data2 = {'test2' => 456}
    @job.input(:in).write(data2)
    @job.run_inline
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq data
    expect(@job.output(:out).read).to eq data2
    expect(@job.status).to eq 'completed'
  end

  it 'passes on default value' do
    @job.input(:in).options = { default: [1,2,3] }
    @job.run_inline
    expect(@job.output(:out).default).to eq [1,2,3]
    expect(@job.status).to eq 'completed'
  end
end
