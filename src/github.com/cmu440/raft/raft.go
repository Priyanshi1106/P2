//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = Make(...)
//   Create a new Raft peer.
//
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (e.g. tester) on the
//   same peer, via the applyCh channel passed to Make()
//

import (
	"fmt"
	"github.com/cmu440/rpc"
	"math"
	"math/rand"
	"sync"
	"time"
)

//
// ApplyMsg
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same peer, via the applyCh passed to Make()
//
type ApplyMsg struct {
	Term    int
	Index   int
	Command interface{}
}

type LogEntry struct {
	Index      int
	Replicated int
	Committed  bool
	Message    ApplyMsg
}

type State int

const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux     sync.Mutex       // Lock to protect shared access to this peer's state
	peers   []*rpc.ClientEnd // RPC end points of all peers
	me      int              // this peer's index into peers[]
	applyCh chan ApplyMsg
	timeout int
	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	currentTerm int
	votedFor    int
	log         []*LogEntry // Should I make another struct?
	lastTerm    int
	lastIndex   int
	voteCount   int
	state       State
	leaderId    int

	commitIndex int
	lastApplied int
	killRoutines bool
	isLeader            bool
	nextIndex           []int
	matchIndex          []int
	leaderElected       chan bool
	receivedMsg         chan bool
	receivedVoteReply   chan RequestVoteReply
	receivedAppendReply chan AppendEntriesReply
	newCommand          chan LogEntry
	killTimeout         chan bool
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {

	rf.mux.Lock()
	me := rf.me
	term := rf.currentTerm
	isLeader := rf.isLeader
	fmt.Printf("GET STATE return %d %d %v\n", me, term, isLeader)
	rf.mux.Unlock()
	return me, term, isLeader
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
//
type RequestVoteReply struct {
	// Your data here (2A)
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      *LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	// If requestVote comes, check:
	rf.receivedMsg <- true
	rf.mux.Lock()
	fmt.Printf("Server %d: Received request vote from %d \n", rf.me, args.CandidateId)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		fmt.Println("Case 1")
		rf.mux.Unlock()
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.lastTerm == args.LastLogTerm && rf.lastIndex == args.LastLogIndex {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			fmt.Println("Case 2")
			rf.mux.Unlock()
			return
		}
	}
	if rf.currentTerm < args.Term {
		if rf.lastTerm <= args.LastLogTerm && rf.lastIndex <= args.LastLogIndex {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			fmt.Println("Case 4")
			rf.mux.Unlock()
			return
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	fmt.Println("Case 3 voted for ", rf.votedFor)
	rf.mux.Unlock()
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	if rf.leaderId == args.LeaderId {
		rf.mux.Unlock()
		rf.receivedMsg <- true
		rf.mux.Lock()
	}
	//fmt.Printf("Term received %d and current term %d\n", args.Term, rf.currentTerm)
	if args.Term < rf.currentTerm {
		fmt.Printf("Server %d at term %d got stale message of term %d from %d \n", rf.me, rf.currentTerm, args.Term, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mux.Unlock()
		return
	} else {
		// What if the entries are nil?
		//var value ApplyMsg{}
		matched := false
		if args.Entries.Message.Command != nil {
			//if rf.log.Len() != 0 {
			//fmt.Printf("Should happen!!! %d %d \n", args.PrevLogIndex, args.PrevLogTerm)
			if args.PrevLogIndex != 0 && args.PrevLogTerm != 0 {
				if args.PrevLogIndex > len(rf.log) {
					reply.Term = rf.currentTerm
					reply.Success = false
					rf.mux.Unlock()
					//rf.receivedMsg <- true
					return
				}
				value := rf.log[args.PrevLogIndex - 1]
				if value.Index == args.PrevLogIndex {
					if value.Message.Term != args.PrevLogTerm {
						fmt.Println("Nooo!!!!!")
						reply.Term = rf.currentTerm
						reply.Success = false
						rf.mux.Unlock()
						//rf.receivedMsg <- true
						return
					}
				} else {
					for i := range rf.log {
						if rf.log[i].Index == args.PrevLogIndex {
							if value.Message.Term != args.PrevLogTerm {
								fmt.Println("Longer Nooo!!!!!")
								reply.Term = rf.currentTerm
								reply.Success = false
								rf.mux.Unlock()
								//rf.receivedMsg <- true
								return
							}
						}
					}
				}
			}
			//fmt.Println("Entry ", len(rf.log))
			if len(rf.log) > args.Entries.Index-1 {
				value := rf.log[args.Entries.Index-1]
				if value.Index == args.Entries.Index {
					if args.Entries.Message.Term != value.Message.Term {
						matched = true
					} else {
						fmt.Println("Finally found you")
						reply.Term = rf.currentTerm
						reply.Success = true
						rf.mux.Unlock()
						return
					}
				}
				if matched {
					rf.log = rf.log[:args.Entries.Index-2]
				}
			}
			//fmt.Printf("Yes!!! %d %v \n", args.Entries.Message.Term, args.Entries.Message.Command)
			// Update lastTerm and lastIndex for rf
			rf.log = append(rf.log, args.Entries)
			rf.lastTerm = args.Entries.Message.Term
			rf.lastIndex = args.Entries.Index
			// for i := range rf.log {
			// 	fmt.Printf("LOG FOR %d SERVER %d place %v\n", rf.me, i, rf.log[i])
			// }

			//rf.lastTerm =
			reply.Term = rf.currentTerm
			reply.Success = true
			rf.mux.Unlock()
			return
		} else {
			if args.PrevLogIndex > len(rf.log) {
				reply.Term = rf.currentTerm
				reply.Success = false
				rf.mux.Unlock()
				//rf.receivedMsg <- true
				return
			}
			//fmt.Printf("Server %d: Updated term %d to %d\n", rf.me, rf.currentTerm, args.Term)
			rf.state = Follower
			rf.leaderId = args.LeaderId
			rf.currentTerm = args.Term
			if args.LeaderCommit > rf.commitIndex {
				oldCommit := rf.commitIndex
				fmt.Printf("COMMIT %d Updating %d ", rf.me, rf.commitIndex)
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(args.PrevLogIndex+1)))
					for oldCommit = oldCommit + 1; oldCommit <= rf.commitIndex; oldCommit++ {
						if oldCommit - 1 < len(rf.log) {
							fmt.Printf("MESSGAE %d on SERVER %d ", oldCommit-1, rf.me)
							value := rf.log[oldCommit - 1]
							fmt.Printf(" %v \n", value.Message.Command)
							rf.mux.Unlock()
							rf.applyCh <- value.Message
							rf.mux.Lock()
						}
					}
				fmt.Printf("to %d \n", rf.commitIndex)
			}
			reply.Term = rf.currentTerm
			reply.Success = true
			rf.mux.Unlock()
			//rf.receivedMsg <- true
			return
		}
	}
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a peer
//
// peer int -- index of the target peer in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which peers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead peer, a live peer that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the peer side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	for {
		ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
		if ok {
			rf.receivedVoteReply <- *reply
			return ok
		}
		rf.mux.Lock()
		if rf.killRoutines {
			rf.mux.Unlock()
			return ok
		}
		rf.mux.Unlock()
	}
}

func (rf *Raft) sendHeartbeatEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	for {
		ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
		if ok {
			rf.receivedAppendReply <- *reply
			return ok
		}
		rf.mux.Lock()
		if rf.killRoutines {
			rf.mux.Unlock()
			return ok
		}
		rf.mux.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	for {
		ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
		if ok {
			//fmt.Println("RECEIVED ", reply.Success)
			if reply.Success {
				rf.mux.Lock()
				rf.nextIndex[peer] = args.Entries.Index + 1
				message := rf.log[args.Entries.Index - 1]
				fmt.Printf("Found message %d %d %v\n", message.Index, message.Message.Term, message.Message.Command)
				if message.Index == args.Entries.Index {
					rf.matchIndex[peer] = rf.nextIndex[peer] - 1
					fmt.Println("Leader received correct reply", rf.matchIndex[peer])
					//message.Replicated++
					//rf.log[args.Entries.Index - 1] = message
				}
				//fmt.Printf("NEED REPLIES %d\n", len(rf.peers)/2)
				count := 1
				index := rf.nextIndex[peer] - 1
				for i := 0; i < len(rf.peers); i++ {
					//fmt.Println("Inside loop ", rf.matchIndex[i])
					if rf.matchIndex[i] == index {
						count++
					}
				}
				if count > len(rf.peers) / 2 && index > rf.commitIndex {
				//if message.Replicated > len(rf.peers)/2 && !message.Committed {
					if message.Index - rf.commitIndex > 1 {
						// rf.mux.Unlock()
						// return ok
					} else {
						fmt.Printf("Leader committed entry %v with index %d\n ", message.Message.Command, message.Index)
						rf.commitIndex = message.Index
						//message.Committed = true
						rf.log[args.Entries.Index - 1] = message
						rf.mux.Unlock()
						rf.applyCh <- message.Message
						return ok
					}
				}
				if rf.nextIndex[peer] < rf.lastIndex + 1 && rf.isLeader {
					//fmt.Println("NEXT ENTRY TO DATE")
					if rf.nextIndex[peer] >= 2 {
						value := rf.log[rf.nextIndex[peer] - 1]
						prevValue := rf.log[rf.nextIndex[peer] - 2]
						fmt.Printf("SENDING TRUE OLD ENTRIES to %d %d mess %v", peer, rf.nextIndex[peer], value)
						args = &AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[peer] - 1, prevValue.Message.Term, value, rf.commitIndex}
					} else if rf.nextIndex[peer] == 1 {
						fmt.Println("SENDING TRUE LAST ENTRY")
						value := rf.log[rf.nextIndex[peer] - 1]
						args = &AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, value, rf.commitIndex}
					}
					// value := rf.log[rf.nextIndex[peer] - 1]
					// prevValue := rf.log[rf.nextIndex[peer] - 2]
					// args = &AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[peer] - 1, prevValue.Message.Term, value, rf.commitIndex}
					rf.mux.Unlock()
					continue
				}
				rf.mux.Unlock()
				return ok
			} else {
				rf.mux.Lock()
				if reply.Term > rf.currentTerm {
					rf.leaderId = -1
					rf.voteCount = 0
					rf.votedFor = -1
					rf.state = Follower
					rf.isLeader = false
					rf.currentTerm = reply.Term
					rf.mux.Unlock()
					return ok
				} else if rf.isLeader {
					rf.nextIndex[peer]--
					if rf.nextIndex[peer] >= 2 {
						//fmt.Println("SENDING FALSE OLD ENTRIES ", rf.nextIndex[peer])
						value := rf.log[rf.nextIndex[peer] - 1]
						prevValue := rf.log[rf.nextIndex[peer] - 2]
						fmt.Printf("SENDING FALSE OLD ENTRIES to %d %d mess %v", peer, rf.nextIndex[peer], value)
						args = &AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[peer] - 1, prevValue.Message.Term, value, rf.commitIndex}
					} else if rf.nextIndex[peer] == 1 {
						fmt.Println("SENDING FALSE LAST ENTRY")
						value := rf.log[rf.nextIndex[peer] - 1]
						args = &AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, value, rf.commitIndex}
					}
				} else if !rf.isLeader {
					fmt.Println("No longer leader")
					rf.mux.Unlock()
					return ok
				}
				rf.mux.Unlock()
			}
		}
		rf.mux.Lock()
		if rf.killRoutines {
			rf.mux.Unlock()
			return ok
		}
		rf.mux.Unlock()
	}
}

//
// Start
// =====
//
// The service using Raft (e.g. a k/v peer) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this peer is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this peer believes it is
// the leader
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mux.Lock()
	if !rf.isLeader {
//		fmt.Println("Not leader")
		rf.mux.Unlock()
		return 0, 0, false
	}
	rf.lastIndex++
	rf.lastTerm = rf.currentTerm
	fmt.Println("Command ", command)
	newCmd := ApplyMsg{rf.currentTerm, rf.lastIndex, command}
	newLogEntry := &LogEntry{rf.lastIndex, 1, false, newCmd}
	rf.log = append(rf.log, newLogEntry)
	fmt.Println("Command added to log")
	term := rf.currentTerm
	index := rf.lastIndex
	isLeader := rf.isLeader
	fmt.Printf("Command added to log %d %d %v\n", term, index, isLeader)
	rf.mux.Unlock()
	rf.newCommand <- *newLogEntry
	return index, term, isLeader
}

//
// Kill
// ====
//
// The tester calls Kill() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Kill() {
	// Your code here, if desired
	rf.killTimeout <- true
}

//
// Make
// ====
//
// The service or tester wants to create a Raft peer
//
// The port numbers of all the Raft peers (including this one)
// are in peers[]
//
// This peer's port is peers[me]
//
// All the peers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyMsg messages
//
// Make() must return quickly, so it should start Goroutines
// for any long-running work
//
func Make(peers []*rpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	fmt.Println("Make called for ", me)
	rf := Raft{}
	rf.peers = peers
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B)
	// Should be the same always. On all servers.
	// Reset on timeout
	rf.currentTerm = 0
	rf.votedFor = -1
	// Should this be a list? Cuz we clearly don't have a length for this.
	//rf.log= list.New() // Should I make another struct?
	rf.log = make([]*LogEntry, 0, 2)
	// TODO: One of these starts with 1
	rf.lastTerm = 0
	rf.lastIndex = 0
	rf.timeout = random(500, 1000)
	// Changes randomly
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteCount = 0
	rf.state = Follower
	rf.leaderId = -1
	// Leader state (reinitialised on election)

	// TODO: What should be the length here?
	rf.isLeader = false
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.killRoutines = false
	rf.leaderElected = make(chan bool)
	rf.receivedMsg = make(chan bool)
	rf.receivedVoteReply = make(chan RequestVoteReply, len(peers))
	rf.receivedAppendReply = make(chan AppendEntriesReply, len(peers))
	rf.killTimeout = make(chan bool)
	rf.newCommand = make(chan LogEntry)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}

	fmt.Println("Make finished ", rf.timeout)
	go rf.timeout_routine()

	return &rf
}

func (rf *Raft) timeout_routine() {
	rf.mux.Lock()
	fmt.Println("Reached here  ", rf.state)
	rf.mux.Unlock()
	//count := 0
	ticker := time.NewTicker(time.Duration(random(700, 1200)) * time.Millisecond)
	ticker.Stop()
	ticker = time.NewTicker(time.Duration(random(700, 1200)) * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			rf.mux.Lock()
			fmt.Printf("Timer expired %d in state %v\n", rf.me, rf.state)
			if rf.state == Follower || rf.state == Candidate {

				rf.state = Candidate
				rf.isLeader = false
				fmt.Println("Starting election for candidate ", rf.me)
				ticker.Stop()
				ticker = time.NewTicker(time.Duration(random(700, 1200)) * time.Millisecond)
				rf.mux.Unlock()
				rf.startNewElection()

			} else {
				fmt.Println("Sending heartbeats: leader ", rf.me)
				ticker.Stop()
				ticker = time.NewTicker(time.Duration(100) * time.Millisecond)
				rf.mux.Unlock()
				rf.sendHeartbeats()

			}
		case <-rf.receivedMsg:
			rf.mux.Lock()
			fmt.Println("Received message server ", rf.me)
			if rf.state == Follower || rf.state == Candidate {
				rf.isLeader = false
				//fmt.Println("Resetting timer")

				ticker.Stop()
				ticker = time.NewTicker(time.Duration(random(700, 1200)) * time.Millisecond)

			}
			rf.mux.Unlock()
		case reply := <-rf.receivedVoteReply:
			rf.mux.Lock()
			if rf.state == Candidate {
				if reply.VoteGranted {
					fmt.Printf("Received true for %d\n", rf.me)
					rf.voteCount++
				} else {
					if reply.Term > rf.currentTerm {
						rf.voteCount = 0
						rf.votedFor = -1
						rf.state = Follower
						rf.isLeader = false
					}
				}
			}
			if rf.voteCount > len(rf.peers)/2 {
				rf.leaderId = rf.me
				rf.votedFor = -1
				rf.voteCount = 0
				rf.state = Leader
				rf.isLeader = true

				// Initilising leader specific values.
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.lastIndex + 1
					rf.matchIndex[i] = 0
				}

				fmt.Println("Sending heartbeats ", rf.me)
				//rf.mux.Unlock()
				ticker.Stop()
				ticker = time.NewTicker(time.Duration(100) * time.Millisecond)
				rf.mux.Unlock()
				rf.sendHeartbeats()

			} else {
				rf.mux.Unlock()
			}
		case reply := <-rf.receivedAppendReply:
			rf.mux.Lock()
			if rf.state == Leader {
				fmt.Println("Received Reply to append Entry")
				if reply.Success {
					//fmt.Println("Received true")
				} else {
					fmt.Printf("Received false %d %d \n", reply.Term, rf.currentTerm)
					if reply.Term > rf.currentTerm {
						rf.leaderId = -1
						rf.voteCount = 0
						rf.votedFor = -1
						rf.state = Follower
						rf.isLeader = false
						rf.currentTerm = reply.Term
					}
				}
			}
			rf.mux.Unlock()
		case newMessage := <-rf.newCommand:
			rf.mux.Lock()
			if rf.isLeader {
				fmt.Println("Leader sending append entries ", newMessage.Message.Command)
				var args *AppendEntriesArgs
				if rf.lastIndex - 1 == 0 {
					args = &AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, &newMessage, rf.commitIndex}
				} else {
					args = &AppendEntriesArgs{rf.currentTerm, rf.me, rf.lastIndex - 1, rf.log[rf.lastIndex - 2].Message.Term, &newMessage, rf.commitIndex}
				}
				//args := &AppendEntriesArgs{rf.currentTerm, rf.me, rf.lastIndex, rf.lastTerm, newMessage, rf.commitIndex}
				len := len(rf.peers)
				me := rf.me
				rf.mux.Unlock()
				for i := 0; i < len; i++ {
					if i != me {
						go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
					}
				}
			}
		case <- rf.killTimeout:
			rf.mux.Lock()
			rf.killRoutines = true
			rf.mux.Unlock()
		}
	}
}

// Generate a random number for timeout
func random(min, max int) int {
	rand.Seed(time.Now().UTC().UnixNano())
	return rand.Intn(max-min) + min
}

func (rf *Raft) startNewElection() {
	rf.mux.Lock()
	fmt.Println("Send vote requests")
	rf.currentTerm++
	rf.state = Candidate
	rf.voteCount = 1
	rf.votedFor = rf.me
	args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.lastIndex, rf.lastTerm}
	len := len(rf.peers)
	me := rf.me
	rf.mux.Unlock()
	for i := 0; i < len; i++ {
		if i != me {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mux.Lock()
	//fmt.Println("Send Heartbeats", rf.lastIndex - 1)
	var args *AppendEntriesArgs
	if rf.lastIndex == 0 || rf.lastIndex - 1 == 0 {
		args = &AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, &LogEntry{}, rf.commitIndex}
	} else {
		args = &AppendEntriesArgs{rf.currentTerm, rf.me, rf.lastIndex - 1, rf.log[rf.lastIndex - 2].Message.Term, &LogEntry{}, rf.commitIndex}
	}
	len := len(rf.peers)
	me := rf.me
	rf.mux.Unlock()
	for i := 0; i < len; i++ {
		if i != me {
			go rf.sendHeartbeatEntries(i, args, &AppendEntriesReply{})
		}
	}
}
