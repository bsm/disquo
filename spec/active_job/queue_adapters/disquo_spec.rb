require 'spec_helper'

RSpec.describe ActiveJob::QueueAdapters::DisquoAdapter do

  subject do
    TestActiveJob.set(wait: 60).perform_later("foo", 1)
  end

  let(:job_data) { show(subject.provider_job_id) }

  it "should enqueue" do
    expect(job_data).to include(
      "additional-deliveries" => 0,
      "id"    => subject.provider_job_id,
      "nacks" => 0,
      "queue" => "__disquo_test__",
      "state" => "active",
    )
    expect(job_data["delay"]).to be_within(5).of(60)
    expect(job_data["retry"]).to be_within(5).of(360)
    expect(job_data["ttl"]).to be_within(5).of(3600)
    expect(Disquo.load_job(job_data["body"])).to eq([
      "ActiveJob::QueueAdapters::DisquoAdapter::JobWrapper",
      [{
        "job_class"  => "TestActiveJob",
        "job_id"     => subject.job_id,
        "provider_job_id" => nil,
        "queue_name" => "__disquo_test__",
        "priority"   => nil,
        "arguments"  => ["foo", 1],
        "executions" => 0,
        "locale"     => "en",
        "disquo"     => {"ttl" => 3600},
      }],
    ])
  end

  it 'should perform' do
    klass, args = Disquo.load_job(job_data["body"])
    Object.const_get(klass).new.perform(*args)

    expect(Disquo::TEST::PERFORMED.size).to eq(1)
    expect(Disquo::TEST::PERFORMED.last).to include(
      klass:  "TestActiveJob",
      args:   ["foo", 1],
    )
  end

end
