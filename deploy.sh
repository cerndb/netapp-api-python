#!/bin/bash
#set -e # Exit with nonzero exit code if anything fails
#set -v
#set -x

SOURCE_BRANCH="master"
TARGET_BRANCH="gh-pages"

TARGET=$1
PAGES_REPO_DIR="deploy_repo"

rm -rf $PAGES_REPO_DIR

pwd

# Pull requests and commits to other branches shouldn't try to deploy, just build to verify
if [ "$TRAVIS_PULL_REQUEST" != "false" -o "$TRAVIS_BRANCH" != "$SOURCE_BRANCH" ]; then
    echo "Skipping deploy"
    exit 0
fi

# Get the deploy key by using Travis's stored variables to decrypt deploy_key.enc
ENCRYPTED_KEY_VAR="encrypted_${ENCRYPTION_LABEL}_key"
ENCRYPTED_IV_VAR="encrypted_${ENCRYPTION_LABEL}_iv"
ENCRYPTED_KEY=${!ENCRYPTED_KEY_VAR}
ENCRYPTED_IV=${!ENCRYPTED_IV_VAR}
openssl aes-256-cbc -K $ENCRYPTED_KEY -iv $ENCRYPTED_IV -in deploy_github_netapp.enc -out deploy_key -d
chmod 600 deploy_key
eval `ssh-agent -s`
ssh-add deploy_key

# Save some useful information
REPO=`git config remote.origin.url`
SSH_REPO=${REPO/https:\/\/github.com\//git@github.com:}
SHA=`git rev-parse --verify HEAD`

# Clone the existing gh-pages for this repo into out/
# Create a new empty branch if gh-pages doesn't exist yet (should only happen on first deply)
git clone $REPO $PAGES_REPO_DIR

cd $PAGES_REPO_DIR
git checkout $TARGET_BRANCH || git checkout --orphan $TARGET_BRANCH

rm -rf ./**/*

# Disable Jekyll
touch .nojekyll

# Now let's go have some fun with the cloned repo
git config user.name "Travis CI"
git config user.email "$COMMIT_AUTHOR_EMAIL"

rm -rf ./**/*

cp -r ../$TARGET/* .

git add .
git diff --staged --exit-code

# If there are no changes to the compiled out (e.g. this is a README update) then just bail.
if [ $? -eq 0 ]; then
    echo "No changes to the output on this push; exiting."
    exit 0
fi

# Commit the "changes", i.e. the new version.
# The delta will show diffs between new and old versions.
git commit -a -m "Deploy to GitHub Pages: ${SHA}"

# Now that we're all set up, we can push.
git push $SSH_REPO $TARGET_BRANCH
