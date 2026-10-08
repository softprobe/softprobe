#!/usr/bin/env bash

set -euo pipefail

manifest=${COMPAT_REFERENCE_MANIFEST:-docs/compat/references.v0.yaml}
test -f "$manifest" || { echo "missing compatibility reference manifest: $manifest" >&2; exit 1; }

ruby -ryaml - "$manifest" <<'RUBY'
manifest = YAML.load_file(ARGV.fetch(0))
manifest.fetch("references").each do |name, reference|
  next if reference["runtime"] == false
  prefix = name.upcase
  image = reference.fetch("image")
  tag = reference.fetch("tag")
  digest = reference["digest"]
  if !digest.is_a?(String) || digest !~ /\Asha256:[0-9a-f]{64}\z/
    abort "#{name} reference must use an immutable sha256 digest"
  end
  if image.include?("@")
    image, embedded_digest = image.split("@", 2)
    abort "#{name} reference image contains an invalid digest" unless embedded_digest&.match?(/\Asha256:[0-9a-f]{64}\z/)
    abort "#{name} reference image and digest disagree" unless embedded_digest == digest
  end
  abort "#{name} reference image must be a repository name" if image.empty? || image.split("/").last.include?(":")
  immutable = "#{image}@#{digest}"
  puts "BASELINE_#{prefix}_IMAGE=#{immutable}"
  puts "BASELINE_#{prefix}_VERSION=#{tag}"
  puts "BASELINE_#{prefix}_DIGEST=#{digest}"
  puts "BASELINE_#{prefix}_REFERENCE=#{immutable}"
end
RUBY
