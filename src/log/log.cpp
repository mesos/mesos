// TODO(benh): Optimize LearnedMessage (and the "commit" stage in
// general) by figuring out a way to not send the entire action
// contents a second time (should cut bandwidth used in half).

// TODO(benh): Provide a LearnRequest that requests more than one
// position at a time, and a LearnResponse that returns as many
// positions as it knows.

// TODO(benh): Implement background catchup: have a new replica that
// comes online become part of the group but don't respond to promises
// or writes until it has caught up! The advantage to becoming part of
// the group is that the new replica can see where the end of the log
// is in order to continue to catch up.

// TODO(benh): Add tests that deliberatly put the system in a state of
// inconsistency by doing funky things to the underlying logs. Figure
// out ways of bringing new replicas online that seem to check the
// consistency of the other replicas.
