#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "yaml"

capability_path, fixture_path, references_path = ARGV
abort "usage: validate.rb CAPABILITY FIXTURE REFERENCES" unless capability_path && fixture_path && references_path

capability = YAML.load_file(capability_path)
fixture = JSON.parse(File.read(fixture_path))
references = YAML.load_file(references_path)

references.fetch("references").each do |name, reference|
  if reference["runtime"] == false
    abort "#{name} non-runtime reference must not declare a digest" if reference.key?("digest")
    next
  end
  digest = reference["digest"]
  abort "#{name} reference must use an immutable sha256 digest" unless
    reference["image"].is_a?(String) && !reference["image"].empty? &&
    reference["tag"].is_a?(String) && !reference["tag"].empty? &&
    digest.is_a?(String) && digest.match?(/\Asha256:[0-9a-f]{64}\z/)
end

tempo = capability.fetch("protocols").fetch("tempo")
contract = tempo.fetch("fixture_contract")
fixture_capability = fixture.fetch("capability")

%w[protocol phase supported_endpoints supported_features unsupported_features fidelity_gaps].each do |key|
  expected = contract.fetch(key)
  actual = fixture_capability.fetch(key)
  abort "Tempo fixture drift for #{key}: expected #{expected.inspect}, got #{actual.inspect}" unless actual == expected
end

tempo_reference = references.fetch("references").fetch("tempo")
expected_image = "#{tempo_reference.fetch("image")}@#{tempo_reference.fetch("digest")}"
evidence = fixture.fetch("evidence")
abort "Tempo fixture reference manifest drift" unless evidence.fetch("reference_manifest") == "docs/compat/references.v0.yaml"
abort "Tempo fixture reference image drift: expected #{expected_image}, got #{evidence.fetch("reference_image")}" unless evidence.fetch("reference_image") == expected_image

scope_fields = contract.fetch("instrumentation_scope_query_fields")
scope_cases = fixture.fetch("cases").select { |item| item.fetch("id").start_with?("tempo-search-instrumentation-") }
scope_queries = scope_cases.map { |item| item.fetch("params").fetch("q") }
scope_fields.each do |field|
  expected_value = field == "name" ? "tempo.phase3.fixture" : "1.0.0"
  expected_query = "{ instrumentation.#{field} = \"#{expected_value}\" }"
  abort "Tempo fixture missing instrumentation.#{field} selector case" unless scope_queries.include?(expected_query)
end

puts "Tempo compatibility metadata matches canonical docs and pinned references"
