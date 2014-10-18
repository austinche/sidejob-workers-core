require 'spec_helper'

describe Workers::Wait do
  it 'can wait for two inputs' do
    @job = SideJob.queue('core', 'Workers::Wait', inports: {port1: {}, port2: {}}, outports: {port1: {}, port2: {}})
    @job.input(:port1).write 'a'
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:port1).data?).to be false
    expect(@job.output(:port2).data?).to be false
    @job.input(:port2).write 'b'
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:port1).read).to eq 'a'
    expect(@job.output(:port2).read).to eq 'b'
  end

  it 'can be used as a trigger by using memory ports' do
    @job = SideJob.queue('core', 'Workers::Wait', inports: {trigger: {}, data: {mode: :memory}}, outports: {data: {}})
    @job.input(:data).write [1,2]
    5.times { @job.input(:trigger).write true }
    @job.run_inline
    expect(@job.status).to eq 'completed'
    5.times { expect(@job.output(:data).read).to eq [1,2] }
  end
end
