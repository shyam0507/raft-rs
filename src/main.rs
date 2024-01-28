use random_number::{rand::rngs::adapter::ReseedingRng, random};
use serde::{Deserialize, Serialize};
use std::{
    env::{self, args},
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread::{self, sleep},
    time::{Duration, Instant},
};

#[derive(Debug, PartialEq)]
enum ServerState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
struct Log {
    term: u32,
    k: String,
    v: String,
}

#[derive(Serialize, Deserialize)]
struct RequestVotePayload {
    term: u64,
    candidate_id: u32,
    last_log_index: u64,
    last_log_term: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct VoteResponse {
    server_id: u32,
    term: u64,
    vote_granted: bool,
}

#[derive(Debug)]
struct Server {
    id: u32,
    current_term: u64,
    voted_for: Option<u32>,
    log: Vec<Log>,
    next_index: Option<u64>,
    match_index: Option<u64>,
    host: String,
    state: ServerState,
    leader_last_hearbeat: Instant,
    election_timeout: Duration,
    port: u32,
    peer_addr: Vec<String>,
    votes_response: Vec<VoteResponse>,
}

impl Server {
    fn init(id: u32, host: String, port: u32, peers: &Vec<String>) -> Self {
        let election_timeout = Duration::from_millis(random!(1000..=3500));
        let server_addr = format!("127.0.0.1:{}", port);
        let mut peer_addr: Vec<String> = vec![];

        for peer in peers {
            if peer != &server_addr {
                peer_addr.push(peer.to_string())
            }
        }

        Server {
            id,
            current_term: 0,
            voted_for: None,
            log: vec![],
            next_index: None,
            match_index: None,
            host,
            state: ServerState::Follower,
            leader_last_hearbeat: Instant::now(),
            election_timeout,
            port,
            peer_addr,
            votes_response: vec![],
        }
    }

    fn append_entry(&self, entries: Vec<Log>) {}
    fn request_vote(&mut self) {
        self.state = ServerState::Candidate;
        self.voted_for = Some(self.id);

        //prepare request vote payload
        let request_payload = RequestVotePayload {
            candidate_id: self.id,
            // last_log_index: (self.log.len()) as u64 - 1,
            // last_log_term: self.log[self.log.len() - 1].term,
            last_log_index: 0,
            last_log_term: 0,
            term: self.current_term,
        };

        let string_json = serde_json::to_string(&request_payload).unwrap() + "\n";
        //request vote from peers
        for peer in &self.peer_addr {
            //TODO parallelize this
            let mut stream = TcpStream::connect(peer).unwrap();
            let _ = stream.write_all(&string_json.as_bytes());

            let mut buf_reader = BufReader::new(&mut stream);
            let mut vote_res_string = String::new();
            buf_reader.read_line(&mut vote_res_string).unwrap();

            let vote_res: VoteResponse = match serde_json::from_str(&vote_res_string.trim()) {
                Ok(response) => response,
                Err(e) => {
                    println!("Failed to deserialize the response: {}", e);
                    continue;
                }
            };

            self.votes_response.push(vote_res)
        }
    }
    fn handle_request_vote_request(&mut self, vote_request: RequestVotePayload) -> VoteResponse {
        //handle request for vote
        //TODO add check for the index and

        if self.voted_for == None && self.current_term <= vote_request.term {
            self.voted_for = Some(vote_request.candidate_id);
            self.current_term = vote_request.term;

            println!(
                "Voted for term {} for server {}",
                vote_request.term, vote_request.candidate_id
            );

            return VoteResponse {
                term: vote_request.term,
                vote_granted: true,
                server_id: self.id,
            };
        } else {
            return VoteResponse {
                term: self.current_term,
                vote_granted: false,
                server_id: self.id,
            };
        }
    }
    fn become_leader(&mut self) {
        self.state = ServerState::Leader;
        println!("Server {} became leader", self.id);
    }
    fn election_timeout_elapsed(&self) -> bool {
        self.leader_last_hearbeat.elapsed() > self.election_timeout
    }
}

fn main() {
    // Get the port from the command line
    let args: Vec<String> = env::args().collect();
    //TODO validations
    let port: u32 = args[1].parse().expect("port is not a num");

    let peers = vec![
        "127.0.0.1:3000".to_string(),
        "127.0.0.1:3001".to_string(),
        "127.0.0.1:3002".to_string(),
    ];

    let server = Arc::new(Mutex::new(Server::init(
        port,
        "127.0.0.1".to_string(),
        port,
        &peers,
    )));
    println!("Server config is {:#?}", server);
    let server_clone = Arc::clone(&server);

    let _ = thread::spawn(|| start_tcp(server_clone));

    loop {
        let mut gaurd = server.lock().unwrap();
        //check if election timeout is greater than since we heard from leader

        if gaurd.state == ServerState::Follower
            && gaurd.election_timeout_elapsed()
            && gaurd.voted_for == None
        {
            println!("heartbeat timedout going for election");
            gaurd.request_vote();
            // drop(gaurd)
        } else if gaurd.state == ServerState::Candidate {
            //check if enough votes received or election timedout
            let mut votes_rec = 0;
            let vote_response = &gaurd.votes_response;
            for vote in vote_response {
                if vote.vote_granted {
                    votes_rec += 1;
                }
            }

            if votes_rec >= peers.len() / 2 {
                gaurd.become_leader();
            } else {
                //TODO implement split vote
            }
        } else {
            drop(gaurd);
        }

        sleep(Duration::from_millis(100))
    }
}

fn start_tcp(server: Arc<Mutex<Server>>) {
    let gaurd = server.lock().unwrap();
    let tcp = TcpListener::bind(format!("127.0.0.1:{}", gaurd.port)).unwrap();
    drop(gaurd);
    println!("TCP Started!");
    for stream in tcp.incoming() {
        let mut stream = stream.unwrap();
        let mut buf_reader = BufReader::new(&mut stream);
        let mut data = String::new();
        buf_reader.read_line(&mut data).unwrap();
        println!("Request: {:#?}", data);

        let mut gaurd = server.lock().unwrap();
        let payload: RequestVotePayload = match serde_json::from_str(&data.trim()) {
            Ok(response) => response,
            Err(e) => {
                println!("Failed to deserialize the response: {}", e);
                continue;
            }
        };

        let vote_response = gaurd.handle_request_vote_request(payload);
        let vote_response_json = serde_json::to_string(&vote_response).unwrap() + "\n";
        let _ = stream.write_all(vote_response_json.as_bytes());

        drop(gaurd)
    }
}
