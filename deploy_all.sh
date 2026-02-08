#!/bin/bash
set -e
echo "Starting deployment sequence..."
git add .
git commit -m "feat(viz): full reanimation of specification evolution with nerve-center metrics and immersive theme"
git push origin main
git push origin main:master
mkdir -p dist
cp visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html dist/index.html
cp spec_evolution_v1.sqlite3 dist/
cp spec_evolution_v1.sqlite3.config.json dist/
cp og-image.png dist/
cp twitter-image.png dist/
cp frankensqlite_illustration.webp dist/
cp frankensqlite_diagram.webp dist/
npx wrangler pages deploy dist --project-name frankensqlite-spec-evolution --branch main --commit-dirty=true
echo "Deployment sequence completed."
