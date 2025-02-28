module did_oracle::oracle {
    use std::string::String;
    use std::signer;
    use std::bcs;
    use std::vector;
    use std::hash;
    use aptos_std::simple_map;
    use aptos_std::smart_vector::{Self, SmartVector};
    use aptos_framework::event;
    use aptos_framework::object;
    use aptos_framework::coin;
    use aptos_framework::transaction_context;
    use aptos_framework::aptos_coin::AptosCoin;
    use aptos_std::type_info::{account_address, type_of, TypeInfo};
    use aptos_framework::account;
    use aptos_framework::timestamp;

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct Roles has key {
        owner: address,
        fee_setter_account: address,
        operation_accounts: SmartVector<address>,
        resource_signer_address: address,
        resource_signer_cap: account::SignerCapability,
        extend_ref: object::ExtendRef,
    }

    struct OracleUAInfo has key {
        ua_address: address,
        ua_info: TypeInfo
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct OracleStore has key {
        expiry_time: u64,
        call_fee: simple_map::SimpleMap<vector<u8>, u64>
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct OvnSupportStore has key {
        support_ovns: simple_map::SimpleMap<vector<u8>, vector<address>>
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct ClaimStore has key {
        claims: simple_map::SimpleMap<vector<u8>, String>
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct ClaimFee has key, copy, store, drop {
        gasAmount: u64,
        free: bool
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct NonceStore has key {
        nonces: simple_map::SimpleMap<address, u64>
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct OracleEventHandleStore has key {
        oracle_request_events: event::EventHandle<OracleRequest>,
        oracle_response_events: event::EventHandle<OracleResponse>,
        oracle_receive_events: event::EventHandle<ReceiveResponse>,
        oracle_cancel_events: event::EventHandle<OracleCancel>,
        oracle_withdraw_events: event::EventHandle<Withdraw>,
        oracle_msg_fee_events: event::EventHandle<MessagingFeesChanged>,
        oracle_fee_setter_events: event::EventHandle<FeeSetterAccountChanged>,
        oracle_operator_events: event::EventHandle<OperatorChanged>,
        oracle_expirytime_events: event::EventHandle<ExpirytimeChanged>,
        oracle_claim_fee_events: event::EventHandle<ClaimFeeChanged>,
        oracle_claim_commited_events: event::EventHandle<ClaimCommited>,
        oracle_job_ovns_events: event::EventHandle<JobOvnsChanged>
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct OracleRequestStore has key {
        request_datas: simple_map::SimpleMap<vector<u8>, RequestData>
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct OracleResponseStore has key {
        response_datas: simple_map::SimpleMap<vector<u8>, String>
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct OracleRequestTags has key {
        request_tags: simple_map::SimpleMap<vector<u8>, bool>
    }

    struct UaCapability<phantom UA> has store, copy, drop {}

    struct RequestData has store, drop {
        sender: address,
        job_id: vector<u8>,
        callback_address: address,
        callback_module: String,
        nonce: u64,
        data: String,
        fee_amount: u64,
        creation_time: u64,
        ovns: simple_map::SimpleMap<address,bool>,
        generate_claim: bool
    }

    const ORACLE_SYMBOL: vector<u8> = b"oracle";

    /// Caller is not authorized to make this call
    const EUNAUTHORIZED: u64 = 1;
    /// Duplicated requestId
    const EDUPLICATED_REQUESTId: u64 = 2;
    /// Not authorized operator
    const ENOT_OPERATOR: u64 = 3;
    /// Invalid requestId
    const EINVALID_REQUESTID: u64 = 4;
    /// Invalid Response
    const EINVALID_RESPONSE: u64 = 5;
    /// Permission denied
    const EPERMISSION_DENIED: u64 = 6;
    /// UA already registered
    const EUA_ALREADY_REGISTERED: u64 = 7;
    /// Length mismatch
    const ELENGHT_MISMATCH: u64 = 8;
    /// Job not exist
    const EJOB_NOT_EXIST: u64 = 9;
    /// Fee Insufficient
    const EFEE_INSUFFICIENT: u64 = 10;
    /// Request Already responded
    const EALREADY_RESPONDED: u64 = 11;
    /// Request not expired
    const EREQUEST_NOT_EXPIRED: u64 = 12;
    /// Request cannot cancel
    const EREQUEST_CANNOT_CANCEL: u64 = 13;
    /// Request Cancel
    const EREQUEST_CANCEL: u64 = 14;
    /// Request cannot refund
    const EREQUEST_CANNOT_REFUND: u64 = 15;
    /// Request Insufficient
    const EREQUEST_INSUFFICIENT: u64 = 16;

    const ECLAIM_EXISTS: u64 = 17;
    /// Claim not found
    const ECLAIM_NOT_FOUND: u64 = 18;
    /// Invalid Claim ID
    const EINVALID_CLAIM_ID: u64 = 19;

    #[event]
    struct ExpirytimeChanged has drop, store {
        before: u64,
        current: u64
    }

    #[event]
    struct OperatorChanged has drop, store {
        operator: address,
        authorized: bool
    }

    #[event]
    struct FeeSetterAccountChanged has drop, store {
        fee_account: address
    }

    #[event]
    struct MessagingFeesChanged has drop, store {
        is_delete: bool,
        job_ids: vector<vector<u8>>,
        amounts: vector<u64>
    }

    #[event]
    struct OracleRequest has drop, store {
        job_id: vector<u8>,
        requester: address,
        request_id: vector<u8>,
        amount: u64,
        callback_address: address,
        callback_module: String,
        data: String,
        ovns: vector<address>,
        generate_claim: bool
    }

    #[event]
    struct OracleCancel has drop, store {
        request_id: vector<u8>,
        refund_amount: u64
    }

    #[event]
    struct OracleResponse has drop, store {
        operator: address,
        request_id: vector<u8>,
        data: String
    }

    #[event]
    struct ReceiveResponse has drop, store {
        call_back: address,
        request_id: vector<u8>,
        ovn: address
    }

    #[event]
    struct Withdraw has drop, store {
        sender: address,
        to: address,
        amount: u64
    }

    #[event]
    struct ClaimFeeChanged has store, drop {
        sender: address,
        before: ClaimFee,
        current: ClaimFee
    }

    #[event]
    struct ClaimCommited has store, drop {
        claim_ID: vector<u8>,
        operator: address
    }

    #[event]
    struct JobOvnsChanged has store, drop {
        sender: address,
        job_id: vector<u8>,
        before: vector<address>,
        current: vector<address>
    }

    #[view]
    public fun oracle_address(): address {
        object::create_object_address(&@did_oracle, ORACLE_SYMBOL)
    }

    #[view]
    public fun owner(): address acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        roles.owner
    }

    #[view]
    public fun get_fee_setter_account(): address acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        roles.fee_setter_account
    }

    #[view]
    public fun get_expiry_time(): u64 acquires OracleStore {
        let store = borrow_global<OracleStore>(oracle_address());
        store.expiry_time
    }

    #[view]
    public fun is_authorized_operator(operator: address): bool acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        smart_vector::contains(&roles.operation_accounts, &operator)
    }

    #[view]
    public fun quote(job_id: vector<u8>, generate_claim: bool): u64 acquires OracleStore, ClaimFee {
        let job_fee = get_messaging_fees(job_id);
        if (generate_claim) {
            job_fee + get_claim_fee().gasAmount
        } else {
            job_fee
        }
    }

    #[view]
    public fun get_messaging_fees(job_id: vector<u8>): u64 acquires OracleStore {
        let store = borrow_global<OracleStore>(oracle_address());
        assert!(simple_map::contains_key(&store.call_fee, &job_id), EJOB_NOT_EXIST);
        *simple_map::borrow(&store.call_fee, &job_id)
    }

    #[view]
    public fun oracle_resource_address(): address acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        roles.resource_signer_address
    }

    #[view]
    public fun oracle_fee_balance(): u64 acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        coin::balance<AptosCoin>(roles.resource_signer_address)
    }

    #[view]
    public fun get_request_data(request_id: vector<u8>): (address, vector<u8>, address, String, vector<address>, String) acquires OracleRequestStore {
        let store = borrow_global<OracleRequestStore>(oracle_address());

        //
        assert!(simple_map::contains_key(&store.request_datas, &request_id), EINVALID_REQUESTID);
        let req = simple_map::borrow(&store.request_datas, &request_id);
        let ovns = vector::empty<address>();
        let keys = simple_map::keys(&req.ovns);
        vector::for_each_ref(&keys, |key| {
            vector::push_back(&mut ovns, *key);
        });
        (req.sender, req.job_id, req.callback_address, req.callback_module, ovns, req.data)
    }

    fun init_module(signer: &signer) {
        let constructor_ref = object::create_named_object(signer, ORACLE_SYMBOL);
        let metadata_object_signer = &object::generate_signer(&constructor_ref);
        let (resource_signer, signer_cap) = account::create_resource_account(signer, b"oracle_signer");
        coin::register<AptosCoin>(&resource_signer);

        let extend_ref = object::generate_extend_ref(&constructor_ref);
        
        move_to(
            metadata_object_signer,
            Roles {
                owner: @owner,
                fee_setter_account: @owner,
                operation_accounts: smart_vector::new(),
                resource_signer_address: signer::address_of(&resource_signer),
                resource_signer_cap: signer_cap,
                extend_ref: extend_ref
            }
        );

        move_to(metadata_object_signer, OracleStore { expiry_time: 0, call_fee: simple_map::create() });

        move_to(
            metadata_object_signer,
            OracleEventHandleStore {
                oracle_request_events: account::new_event_handle<OracleRequest>(&resource_signer),
                oracle_response_events: account::new_event_handle<OracleResponse>(&resource_signer),
                oracle_receive_events: account::new_event_handle<ReceiveResponse>(&resource_signer),
                oracle_cancel_events: account::new_event_handle<OracleCancel>(&resource_signer),
                oracle_withdraw_events: account::new_event_handle<Withdraw>(&resource_signer),
                oracle_msg_fee_events: account::new_event_handle<MessagingFeesChanged>(&resource_signer),
                oracle_fee_setter_events: account::new_event_handle<FeeSetterAccountChanged>(&resource_signer),
                oracle_operator_events: account::new_event_handle<OperatorChanged>(&resource_signer),
                oracle_expirytime_events: account::new_event_handle<ExpirytimeChanged>(&resource_signer),
                oracle_claim_fee_events: account::new_event_handle<ClaimFeeChanged>(&resource_signer),
                oracle_claim_commited_events: account::new_event_handle<ClaimCommited>(&resource_signer),
                oracle_job_ovns_events: account::new_event_handle<JobOvnsChanged>(&resource_signer)
            }
        );

        move_to(metadata_object_signer, OracleRequestStore { request_datas: simple_map::create() });

        move_to(metadata_object_signer, OracleResponseStore { response_datas: simple_map::create() });

        move_to(metadata_object_signer, OvnSupportStore { support_ovns: simple_map::create() });

        move_to(metadata_object_signer, ClaimStore { claims: simple_map::create() });

        move_to(metadata_object_signer, ClaimFee { gasAmount: 0, free: true });

        move_to(metadata_object_signer, NonceStore { nonces: simple_map::create() });
    }

    public fun register_ua<UA>(account: &signer): UaCapability<UA> {
        assert_type_signer<UA>(account);

        assert!(!exists<OracleUAInfo>(signer::address_of(account)), EUA_ALREADY_REGISTERED);
        let type_address = type_address<UA>();
        let type_info = type_of<UA>();

        move_to(account, OracleUAInfo { ua_address: type_address, ua_info: type_info });

        UaCapability<UA> {}
    }

    public entry fun oracle_request_web2(
        sender: &signer,
        job_id: vector<u8>,
        callback_address: address,
        callback_module: String,
        data: String,
        fee: u64,
        ovns: vector<address>,
        generate_claim: bool
    ) acquires Roles, OracleStore, OracleEventHandleStore, OracleRequestStore, OracleRequestTags, NonceStore, ClaimFee {

        let job_fee = coin::withdraw<AptosCoin>(sender, fee);

        let ua_address = signer::address_of(sender);
        let ovns_quantity = vector::length(&ovns);
        let pay_fee = pay_oracle_fee(job_id, job_fee, ovns_quantity, generate_claim);
        let request_id = oracle_request_internal(ua_address, job_id, callback_address, callback_module, data, pay_fee, ovns, generate_claim);

        if (!exists<OracleRequestTags>(ua_address)) {
            move_to(sender, OracleRequestTags { request_tags: simple_map::create() })
        };

        let tags = borrow_global_mut<OracleRequestTags>(ua_address);
        simple_map::upsert(&mut tags.request_tags, request_id, true);

        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event<OracleRequest>(
            &mut event_store.oracle_request_events,
            OracleRequest { job_id, request_id, amount: pay_fee, requester: ua_address, callback_address, callback_module, data, ovns, generate_claim }
        );
    }

    public fun oracle_request<UA>(
        job_id: vector<u8>,
        callback_address: address,
        callback_module: String,
        data: String,
        fee: coin::Coin<AptosCoin>,
        ovns: vector<address>,
        generate_claim: bool,
        _cap: &UaCapability<UA>
    ): (vector<u8>) acquires Roles, OracleStore, OracleEventHandleStore, OracleRequestStore, NonceStore, ClaimFee {
        let ua_address = type_address<UA>();
        let ovns_quantity = vector::length(&ovns);
        let pay_fee = pay_oracle_fee(job_id, fee, ovns_quantity,generate_claim);
        let request_id = oracle_request_internal(ua_address, job_id, callback_address, callback_module, data, pay_fee, ovns, generate_claim);
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event<OracleRequest>(
            &mut event_store.oracle_request_events,
            OracleRequest { job_id, request_id, amount: pay_fee, requester: ua_address, callback_address, callback_module, data, ovns, generate_claim }
        );

        (request_id)
    }

    fun pay_oracle_fee(job_id: vector<u8>, fee: coin::Coin<AptosCoin>, ovns_quantity: u64, generate_claim: bool): u64 acquires Roles, OracleStore, ClaimFee {
        let roles = borrow_global<Roles>(oracle_address());

        let job_fee_gas = quote(job_id, generate_claim);
        
        let pay_fee = coin::value(&fee);

        let gas_price = transaction_context::gas_unit_price();
        //let gas_price= 1;
        let job_fee = job_fee_gas * gas_price * ovns_quantity;

        assert!(pay_fee >= job_fee, EFEE_INSUFFICIENT);

        //let job_fee = coin::extract(&mut fee, job_fee_amout);
        coin::deposit(roles.resource_signer_address, fee);

        pay_fee
    }

    fun oracle_request_internal(
        sender_address: address,
        job_id: vector<u8>,
        callback_address: address,
        callback_module: String,
        data: String,
        fee_amount: u64,
        ovns: vector<address>,
        generate_claim: bool
    ): vector<u8> acquires OracleRequestStore, NonceStore {

        let nonce_store = borrow_global_mut<NonceStore>(oracle_address());
        let nonce = 1;
        if(!simple_map::contains_key(&nonce_store.nonces, &sender_address)) {
            simple_map::add(&mut nonce_store.nonces, sender_address, 1);
        } else {
            let before_nonce = *simple_map::borrow(&nonce_store.nonces, &sender_address);
            nonce = before_nonce + 1;
            simple_map::upsert(&mut nonce_store.nonces, sender_address, nonce);
        };

        let request_id = gen_request_id(&sender_address, nonce);

        let store = borrow_global_mut<OracleRequestStore>(oracle_address());

        assert!(!simple_map::contains_key(&store.request_datas, &request_id), EINVALID_REQUESTID);

        let ovns_map = simple_map::create<address, bool>();
        vector::for_each(ovns, |ovn| {
            simple_map::add(&mut ovns_map, ovn, false);
        });

        simple_map::add(
            &mut store.request_datas,
            request_id,
            RequestData {
                sender: sender_address,
                job_id,
                callback_address,
                callback_module,
                nonce,
                data,
                fee_amount,
                creation_time: timestamp::now_seconds(),
                ovns: ovns_map,
                generate_claim
            }
        );

        request_id
    }

    public entry fun fulfill_oracle_request(
        operator: &signer, 
        request_id: vector<u8>, 
        data: String
    ) acquires Roles, OracleRequestStore, OracleResponseStore, OracleEventHandleStore, OracleRequestTags {
        assert_is_operator(operator);
        let operator_address = signer::address_of(operator);
        let request_store = borrow_global_mut<OracleRequestStore>(oracle_address());
        assert!(simple_map::contains_key(&request_store.request_datas, &request_id), EINVALID_REQUESTID);

        let request_info = simple_map::borrow_mut(&mut request_store.request_datas, &request_id);
        assert!(simple_map::contains_key(&request_info.ovns, &operator_address), ENOT_OPERATOR);
        if (is_web2_sender(request_info.sender, request_id)) {
            *simple_map::borrow_mut(&mut request_info.ovns, &operator_address) = true;
            let all_confirmed = true;
            let keys = simple_map::keys(&request_info.ovns);
            vector::for_each_ref(&keys, |key| {
                all_confirmed = all_confirmed && *simple_map::borrow(&request_info.ovns, key);
            });

            if (all_confirmed) {
                simple_map::remove(&mut request_store.request_datas, &request_id);
            };
        } else {
            let request_hash = get_request_operator_hash(request_id, operator_address);
            let response_store = borrow_global_mut<OracleResponseStore>(oracle_address());
            assert!(!simple_map::contains_key(&response_store.response_datas, &request_hash), EINVALID_REQUESTID);
            simple_map::add(&mut response_store.response_datas, request_hash, data);
        };

        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(
            &mut event_store.oracle_response_events, OracleResponse { operator: signer::address_of(operator), request_id, data }
        );
    }

    fun is_web2_sender(sender: address, request_id: vector<u8>): bool acquires OracleRequestTags {
        if (!exists<OracleRequestTags>(sender)) {
            return false
        };

        let tags = borrow_global_mut<OracleRequestTags>(sender);
        if (!simple_map::contains_key(&tags.request_tags, &request_id)) {
            return false
        };
        simple_map::remove(&mut tags.request_tags, &request_id);

        true
    }

    public fun receive_response<UA>(request_id: vector<u8>, ovn: address, _cap: &UaCapability<UA>): String acquires OracleRequestStore, OracleResponseStore, OracleEventHandleStore {
        let ua_address = type_address<UA>();
        let request_hash = get_request_operator_hash(request_id, ovn);
        let request_store = borrow_global_mut<OracleRequestStore>(oracle_address());
        let response_store = borrow_global_mut<OracleResponseStore>(oracle_address());
        let call_back = simple_map::borrow(&request_store.request_datas, &request_id).callback_address;
        assert!(ua_address == call_back, EINVALID_RESPONSE);
        assert!(simple_map::contains_key(&response_store.response_datas, &request_hash), EINVALID_RESPONSE);
        let request_info = simple_map::borrow_mut(&mut request_store.request_datas, &request_id);
        assert!(simple_map::contains_key(&request_info.ovns, &ovn), EINVALID_REQUESTID);
        *simple_map::borrow_mut(&mut request_info.ovns, &ovn) = true;
        let all_confirmed = true;
        let keys = simple_map::keys(&request_info.ovns);
        vector::for_each_ref(&keys, |key| {
            all_confirmed = all_confirmed && *simple_map::borrow(&request_info.ovns, key);
        });

        if (all_confirmed) {
            simple_map::remove(&mut request_store.request_datas, &request_id);
        };
        
        let res_data = *simple_map::borrow(&response_store.response_datas, &request_hash);
        simple_map::remove(&mut response_store.response_datas, &request_hash);
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(&mut event_store.oracle_receive_events, ReceiveResponse { call_back, request_id, ovn });

        res_data
    }

    public entry fun cancel_oracle_request_web2(
        sender: &signer, request_id: vector<u8>
    ) acquires Roles, OracleStore, OracleRequestStore, OracleResponseStore, OracleEventHandleStore, OracleRequestTags {

        let ua_address = signer::address_of(sender);
        assert!(is_web2_sender(ua_address, request_id), EINVALID_REQUESTID);

        let refund_amount = cancel_oracle_request_internal(ua_address, request_id);

        let roles = borrow_global<Roles>(oracle_address());
        let resource_signer = account::create_signer_with_capability(&roles.resource_signer_cap);
        let balance = coin::balance<AptosCoin>(roles.resource_signer_address);
        assert!(balance >= refund_amount, EREQUEST_INSUFFICIENT);
        let refund_fee = coin::withdraw<AptosCoin>(&resource_signer, refund_amount);
        coin::deposit(ua_address, refund_fee);

    }

    public fun cancel_oracle_request<UA>(
        request_id: vector<u8>, _cap: &UaCapability<UA>
    ): coin::Coin<AptosCoin> acquires Roles, OracleStore, OracleRequestStore, OracleResponseStore, OracleEventHandleStore, OracleRequestTags {
        let ua_address = type_address<UA>();

        assert!(!is_web2_sender(ua_address, request_id), EINVALID_REQUESTID);

        let refund_amount = cancel_oracle_request_internal(ua_address, request_id);

        let roles = borrow_global<Roles>(oracle_address());
        let resource_signer = account::create_signer_with_capability(&roles.resource_signer_cap);
        let balance = coin::balance<AptosCoin>(roles.resource_signer_address);
        assert!(balance >= refund_amount, EREQUEST_INSUFFICIENT);
        let refund_fee = coin::withdraw<AptosCoin>(&resource_signer, refund_amount);

        refund_fee
    }

    fun cancel_oracle_request_internal(ua_address: address, request_id: vector<u8>): u64 acquires OracleRequestStore, OracleResponseStore, OracleEventHandleStore, OracleStore {
        let request_store = borrow_global_mut<OracleRequestStore>(oracle_address());
        let response_store = borrow_global_mut<OracleResponseStore>(oracle_address());

        assert!(!simple_map::contains_key(&response_store.response_datas, &request_id), EALREADY_RESPONDED);
        assert!(simple_map::contains_key(&request_store.request_datas, &request_id), EINVALID_REQUESTID);

        let request_info = simple_map::borrow_mut(&mut request_store.request_datas, &request_id);
        assert!(request_info.sender == ua_address, EINVALID_REQUESTID);

        let keys = simple_map::keys(&request_info.ovns);
        if (vector::length(&keys) > 1) {
            vector::for_each_ref(&keys, |key| {
                assert!(!*simple_map::borrow(&request_info.ovns, key), EREQUEST_CANNOT_CANCEL);
            });
        };
        
        let now = timestamp::now_seconds();
        let store = borrow_global<OracleStore>(oracle_address());
        assert!(now > request_info.creation_time + store.expiry_time, EREQUEST_NOT_EXPIRED);
        let refund_amount = request_info.fee_amount;

        simple_map::remove(&mut request_store.request_datas, &request_id);
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(&mut event_store.oracle_cancel_events, OracleCancel { request_id, refund_amount });

        refund_amount
    }

    public entry fun withdraw_fee(fee_setter: &signer, to: address, amount: u64) acquires Roles, OracleEventHandleStore {
        assert_is_fee_setter(fee_setter);
        let roles = borrow_global<Roles>(oracle_address());
        let resource_signer = account::create_signer_with_capability(&roles.resource_signer_cap);

        let balance = coin::balance<AptosCoin>(roles.resource_signer_address);
        assert!(balance >= amount, EREQUEST_INSUFFICIENT);

        coin::transfer<AptosCoin>(&resource_signer, to, amount);
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(
            &mut event_store.oracle_withdraw_events, Withdraw { sender: signer::address_of(fee_setter), to, amount }
        )
    }

    public entry fun transfer_ownership(owner: &signer, new_owner: address) acquires Roles {
        assert_is_owner(owner);
        let roles = borrow_global_mut<Roles>(oracle_address());
        roles.owner = new_owner
    }

    public entry fun set_expirytime(owner: &signer, expirytime: u64) acquires Roles, OracleStore, OracleEventHandleStore {
        assert_is_owner(owner);
        set_expirytime_internal(expirytime);
    }

    fun set_expirytime_internal(expirytime: u64) acquires OracleStore, OracleEventHandleStore {
        let store = borrow_global_mut<OracleStore>(oracle_address());
        let before = store.expiry_time;
        store.expiry_time = expirytime;
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(&mut event_store.oracle_expirytime_events, ExpirytimeChanged { before, current: expirytime });
    }

    public entry fun set_operator(owner: &signer, operator: address, authorized: bool) acquires Roles, OracleEventHandleStore {
        assert_is_owner(owner);
        set_operator_internal(operator, authorized);
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(&mut event_store.oracle_operator_events, OperatorChanged { operator, authorized });
    }

    fun set_operator_internal(operator: address, authorized: bool) acquires Roles {
        let roles = borrow_global_mut<Roles>(oracle_address());
        let (has_operator, index) = smart_vector::index_of(&roles.operation_accounts, &operator);
        if (has_operator == false && authorized == true) {
            // add operator
            smart_vector::push_back(&mut roles.operation_accounts, operator);
        };

        if (has_operator == true && authorized == false) {
            //delete operator
            smart_vector::remove(&mut roles.operation_accounts, index);
        }
    }

    public entry fun set_fee_setter_account(owner: &signer, fee_account: address) acquires Roles, OracleEventHandleStore {
        assert_is_owner(owner);
        set_fee_setter_account_internal(fee_account);
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(&mut event_store.oracle_fee_setter_events, FeeSetterAccountChanged { fee_account });
    }

    fun set_fee_setter_account_internal(fee_account: address) acquires Roles {
        let roles = borrow_global_mut<Roles>(oracle_address());
        roles.fee_setter_account = fee_account;

    }

    public entry fun set_messaging_fees(owner: &signer, job_ids: vector<vector<u8>>, amounts: vector<u64>, free: vector<bool>) acquires Roles, OracleStore, OracleEventHandleStore {
        assert_is_owner(owner);
        let l = vector::length(&job_ids);
        assert!(l == vector::length(&amounts), ELENGHT_MISMATCH);
        assert!(l == vector::length(&free), ELENGHT_MISMATCH);

        let store = borrow_global_mut<OracleStore>(oracle_address());
        let event_amounts = vector::empty<u64>();
        vector::enumerate_ref(
            &job_ids,
            |i, job_id| {
                let amount = *vector::borrow(&amounts, i);
                let is_free = *vector::borrow(&free, i);
                let final_amount = if (is_free == true) { 0 } else { amount };
                simple_map::upsert(&mut store.call_fee, *job_id, final_amount);
                vector::push_back(&mut event_amounts, final_amount);
            }
        );
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(&mut event_store.oracle_msg_fee_events, MessagingFeesChanged { is_delete: false, job_ids, amounts: event_amounts });
    }

    public entry fun del_messaging_fees(owner: &signer, job_ids: vector<vector<u8>>) acquires Roles, OracleStore, OracleEventHandleStore {
        assert_is_owner(owner);

        let store = borrow_global_mut<OracleStore>(oracle_address());
        vector::for_each(job_ids, |job_id| {
            simple_map::remove(&mut store.call_fee, &job_id);
        });
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(
            &mut event_store.oracle_msg_fee_events,
            MessagingFeesChanged {
                is_delete: true,
                job_ids,
                amounts: vector::empty<u64>()
            }
        );
    }

    public entry fun commit_claim(
        sender: &signer,
        claim_id: vector<u8>,
        claim: String
    ) acquires Roles, ClaimStore, OracleEventHandleStore {
        assert_is_operator(sender);
        assert!(vector::length(&claim_id) != 0, EINVALID_CLAIM_ID);
        let claim_store = borrow_global_mut<ClaimStore>(oracle_address());
        assert!(!simple_map::contains_key(&claim_store.claims, &claim_id), ECLAIM_EXISTS);

        simple_map::add(&mut claim_store.claims, claim_id,claim);
        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(
            &mut event_store.oracle_claim_commited_events,
            ClaimCommited { claim_ID: claim_id, operator: signer::address_of(sender) }
        );
    }

    public entry fun set_claim_fee(
        sender: &signer,
        gas_amount: u64,
        free: bool
    ) acquires Roles, ClaimFee, OracleEventHandleStore {
        assert_is_owner(sender);
        
        let claim_fee = ClaimFee {
            gasAmount: gas_amount,
            free
        };
        
        let claim_fee_store = borrow_global_mut<ClaimFee>(oracle_address());
        let old_claim_fee = *claim_fee_store;
        claim_fee_store.gasAmount = claim_fee.gasAmount;
        claim_fee_store.free = claim_fee.free;

        let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
        event::emit_event(
            &mut event_store.oracle_claim_fee_events,
            ClaimFeeChanged { 
                sender: signer::address_of(sender),
                before: old_claim_fee,
                current: claim_fee
            }
        );
    }

    #[view]
    public fun get_claim_fee(): ClaimFee acquires ClaimFee {
        let claim_fee = borrow_global<ClaimFee>(oracle_address());
        *claim_fee
    }


    #[view]
    public fun get_claim(claim_id: vector<u8>): String acquires ClaimStore {
        let claim_store = borrow_global<ClaimStore>(oracle_address());
        assert!(simple_map::contains_key(&claim_store.claims, &claim_id), ECLAIM_NOT_FOUND);
        *simple_map::borrow(&claim_store.claims, &claim_id)
    }

    public entry fun set_ovns_for_job(sender: &signer, job_ids: vector<vector<u8>>, ovns: vector<vector<address>>) acquires Roles, OvnSupportStore, OracleEventHandleStore {
        assert_is_owner(sender);
        let l = vector::length(&job_ids);
        assert!(l == vector::length(&ovns), ELENGHT_MISMATCH);
        let ovns_map = simple_map::create<vector<u8>, vector<address>>();
        let i = 0;
        while (i < vector::length(&job_ids)) {
            let job_id = *vector::borrow(&job_ids, i);
            let ovn_vector = *vector::borrow(&ovns, i);
            simple_map::add(&mut ovns_map, job_id, ovn_vector);
            i = i + 1;
        };
        let sender_address = signer::address_of(sender);
        set_ovns_for_job_internal(sender_address, ovns_map, sender_address);
    }

    public fun set_ovns_for_job_internal(sender_address: address, ovns_map: simple_map::SimpleMap<vector<u8>, vector<address>>, sender: address) acquires OvnSupportStore, OracleEventHandleStore {
        let store = borrow_global_mut<OvnSupportStore>(oracle_address());
        let jobIds = simple_map::keys(&ovns_map);
        vector::for_each_ref(&jobIds, |job_id| {
            let before = vector::empty<address>();
            let value = *simple_map::borrow(&ovns_map, job_id);
            if (simple_map::contains_key(&store.support_ovns, job_id)) {
                before = *simple_map::borrow(&store.support_ovns, job_id);
                *simple_map::borrow_mut(&mut store.support_ovns, job_id) = value;
            } else {
                simple_map::add(&mut store.support_ovns, *job_id, value);
            };
            let event_store = borrow_global_mut<OracleEventHandleStore>(oracle_address());
            event::emit_event(
                &mut event_store.oracle_job_ovns_events, 
                JobOvnsChanged { 
                    sender: sender_address,
                    job_id: *job_id, 
                    before, 
                    current: value 
                }
            );
        });
    }

    #[view]
    public fun get_ovns_for_job(job_id: vector<u8>): vector<address> acquires OvnSupportStore {
        let store = borrow_global<OvnSupportStore>(oracle_address());
        assert!(simple_map::contains_key(&store.support_ovns, &job_id), EJOB_NOT_EXIST);
        *simple_map::borrow(&store.support_ovns, &job_id)
    }

    fun gen_request_id(sender: &address, nonce: u64): vector<u8> {
        let bytes = bcs::to_bytes(sender);
        vector::append(&mut bytes, bcs::to_bytes(&nonce));
        hash::sha3_256(bytes)
    }

    fun type_address<TYPE>(): address {
        account_address(&type_of<TYPE>())
    }

    fun assert_is_owner(owner: &signer) acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        let owner_addr = signer::address_of(owner);
        assert!(roles.owner == owner_addr, EUNAUTHORIZED);
    }

    fun assert_is_fee_setter(fee_setter: &signer) acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        let fee_setter_addr = signer::address_of(fee_setter);
        assert!(roles.fee_setter_account == fee_setter_addr, EUNAUTHORIZED);
    }

    fun assert_is_operator(operator: &signer) acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        let operator_addr = signer::address_of(operator);
        assert!(smart_vector::contains(&roles.operation_accounts, &operator_addr), EUNAUTHORIZED);
    }

    fun assert_type_signer<TYPE>(account: &signer) {
        assert!(type_address<TYPE>() == signer::address_of(account), EPERMISSION_DENIED);
    }

    #[test_only]
    public fun init_expirytime_for_test(expirytime: u64) acquires OracleStore, OracleEventHandleStore {
        set_expirytime_internal(expirytime);
    }

    #[test_only]
    public fun init_operator_for_test(operator: address) acquires Roles {
        set_operator_internal(operator, true);
    }

    #[test_only]
    public fun init_fee_for_test(fee_account: &signer) acquires Roles {

        let fee_account_address = signer::address_of(fee_account);
        // account::create_account_for_test(fee_account_address);
        // coin::register<AptosCoin>(fee_account);
        set_fee_setter_account_internal(fee_account_address);
    }

    #[test_only]
    public fun init_job_for_test(job_id: vector<u8>, amount: u64) acquires OracleStore {
        let store = borrow_global_mut<OracleStore>(oracle_address());
        simple_map::upsert(&mut store.call_fee, job_id, amount);
    }

    #[test_only]
    public fun init_for_test(signer: &signer) {
        init_module(signer);
    }

    #[view]
    public fun get_request_operator_hash(request_id: vector<u8>, operator_address: address): vector<u8> {
        let bytes = request_id;
        vector::append(&mut bytes, bcs::to_bytes(&operator_address));
        hash::sha3_256(bytes)
    }
}
