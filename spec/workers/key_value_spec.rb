require 'spec_helper'

describe Workers::KeyValue do
  it 'only outputs the specified keys' do
    @job = SideJob.queue('core', 'Workers::KeyValue', outports: {key1: {}, key2: {}})
    @job.input(:in).write({key1: [1,2], key2: 3, key3: true})
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:key1).read).to eq [1,2]
    expect(@job.output(:key2).read).to eq 3
    expect(@job.output(:key3).data?).to be false
  end
end
