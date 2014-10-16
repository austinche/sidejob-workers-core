require 'spec_helper'

describe Workers::Zip do
  before do
    @job = SideJob.queue('core', 'Workers::Zip')
  end

  it 'zips up arrays' do
    @job.input(:in).write [[1,2], [true,false], [{key1: 'val1'}, {key1: 'val2'}]]
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq [[1, true, {'key1' => 'val1'}], [2, false, 'key1' => 'val2']]
  end
end
