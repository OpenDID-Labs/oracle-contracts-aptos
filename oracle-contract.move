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
    use aptos_framework::aptos_coin::AptosCoin;
    use aptos_std::type_info::{account_address, type_of, TypeInfo};
    #[test_only]
    use aptos_framework::account;

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct Roles has key {
        owner: address,
        fee_account: address,
        operation_accounts: SmartVector<address>
    }

    struct OracleUAInfo has key {
        ua_address:address,
        ua_info:TypeInfo,
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct OracleStore has key {
        call_fee: simple_map::SimpleMap<vector<u8>, u64>,
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct OracleRequestStore has key {
        request_datas: simple_map::SimpleMap<vector<u8>, RequestData>,
    }

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    struct OracleResponseStore has key {
        response_datas: simple_map::SimpleMap<vector<u8>, String>
    }

    struct UaCapability<phantom UA> has store, copy, drop {}

    struct RequestData has store, drop {
        sender: address,
        job_id: vector<u8>,
        callback_address: address,
        callback_module: String,
        nonce: u64,
        data: String,

        fee_amount:u64,
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
    const EFEE_INSUFFICIENT :u64 = 10;

    #[event]
    struct OperatorChanged has drop, store {
        operator: address,
        authorized: bool
    }

    #[event]
    struct FeeAccountChanged has drop, store {
        fee_account: address
    }

    #[event]
    struct MessagingFeesChanged has drop,store {
        is_delete:bool,
        job_ids:vector<vector<u8>>,
        amounts:vector<u64>
    }

    #[event]
    struct OracleRequest has drop, store {
        job_id: vector<u8>,
        requester: address,
        request_id: vector<u8>,
        amount:u64,
        callback_address: address,
        callback_module: String,
        data: String
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
        request_id: vector<u8>
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
    public fun get_fee_account(): address acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        roles.fee_account
    }

    #[view]
    public fun is_authorized_operator(operator: address): bool acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        smart_vector::contains(&roles.operation_accounts, &operator)
    }

    #[view]
    public fun get_messaging_fees(job_id: vector<u8>): u64 acquires OracleStore {
        let store = borrow_global<OracleStore>(oracle_address());
        assert!(simple_map::contains_key(&store.call_fee, &job_id), EJOB_NOT_EXIST);
        *simple_map::borrow(&store.call_fee, &job_id)
    }

    #[view]
    public fun get_request_data(request_id: vector<u8>): (address, vector<u8>, address, String, u64, String) acquires OracleRequestStore {
        let store = borrow_global<OracleRequestStore>(oracle_address());

        //
        assert!(simple_map::contains_key(&store.request_datas, &request_id), EINVALID_REQUESTID);
        let req = simple_map::borrow(&store.request_datas, &request_id);

        (req.sender, req.job_id, req.callback_address, req.callback_module, req.nonce, req.data)
    }

    fun init_module(signer: &signer) {
        let constructor_ref = &object::create_named_object(signer, ORACLE_SYMBOL);
        let metadata_object_signer = &object::generate_signer(constructor_ref);

        move_to(metadata_object_signer, Roles { owner: @owner, fee_account: @owner, operation_accounts: smart_vector::new() });

        move_to(
            metadata_object_signer,
            OracleStore {
                call_fee: simple_map::create(),
            }
        );

        move_to(
            metadata_object_signer,
            OracleRequestStore {
                request_datas: simple_map::create(),
            }
        );

        move_to(
            metadata_object_signer,
            OracleResponseStore {
                response_datas: simple_map::create()
            }
        );
    }

    public fun register_ua<UA>(account: &signer): UaCapability<UA> {
        assert_type_signer<UA>(account);

        assert!(!exists<OracleUAInfo>(signer::address_of(account)),EUA_ALREADY_REGISTERED);
        let type_address = type_address<UA>();
        let type_info = type_of<UA>();

        move_to(account,OracleUAInfo{
            ua_address:type_address,
            ua_info:type_info,
        });

        UaCapability<UA> {}
    }

    public fun oracle_request<UA>(
        job_id: vector<u8>,
        callback_address: address,
        callback_module: String,
        nonce: u64,
        data: String,
        fee: coin::Coin<AptosCoin>,
        _cap: &UaCapability<UA>
    ): (vector<u8>) acquires Roles, OracleStore,OracleRequestStore {
        let ua_address = type_address<UA>();
        let pay_fee = pay_oracle_fee(job_id, fee);
        let request_id = oracle_request_internal(ua_address, job_id, callback_address, callback_module, nonce, data,pay_fee);

        event::emit(OracleRequest { job_id, request_id,amount:pay_fee, requester: ua_address, callback_address, callback_module, data });

        (request_id)
    }

    fun pay_oracle_fee(job_id: vector<u8>, fee: coin::Coin<AptosCoin>): u64 acquires Roles, OracleStore {
        let roles = borrow_global<Roles>(oracle_address());

        let job_fee_amout = get_messaging_fees(job_id);
        let pay_fee = coin::value(&fee);
        assert!(pay_fee >= job_fee_amout, EFEE_INSUFFICIENT);

        //let job_fee = coin::extract(&mut fee, job_fee_amout);
        coin::deposit(roles.fee_account, fee);

        pay_fee
    }

    fun oracle_request_internal(
        sender_address: address,
        job_id: vector<u8>,
        callback_address: address,
        callback_module: String,
        nonce: u64,
        data: String,
        fee_amount:u64,
    ): vector<u8> acquires OracleRequestStore {

        let request_id = gen_request_id(&sender_address, nonce);

        let store = borrow_global_mut<OracleRequestStore>(oracle_address());


        assert!(!simple_map::contains_key(&store.request_datas, &request_id), EINVALID_REQUESTID);

        simple_map::add(
            &mut store.request_datas,
            request_id,
            RequestData { sender: sender_address, job_id, callback_address, callback_module, nonce, data,fee_amount }
        );


        request_id
    }

    public entry fun fulfill_oracle_request(operator: &signer, request_id: vector<u8>, data: String) acquires Roles, OracleRequestStore,OracleResponseStore {
        assert_is_operator(operator);
        let request_store = borrow_global<OracleRequestStore>(oracle_address());
        assert!(simple_map::contains_key(&request_store.request_datas, &request_id), EINVALID_REQUESTID);

        let response_store = borrow_global_mut<OracleResponseStore>(oracle_address());
        assert!(!simple_map::contains_key(&response_store.response_datas, &request_id), EINVALID_REQUESTID);

        simple_map::add(&mut response_store.response_datas, request_id, data);

        event::emit(OracleResponse { operator: signer::address_of(operator), request_id, data });
    }

    public fun receive_response<UA>(request_id: vector<u8>, data: String, _cap: &UaCapability<UA>) acquires OracleRequestStore,OracleResponseStore {
        let ua_address = type_address<UA>();

        let request_store = borrow_global_mut<OracleRequestStore>(oracle_address());
        let response_store = borrow_global_mut<OracleResponseStore>(oracle_address());

        assert!(simple_map::contains_key(&response_store.response_datas, &request_id), EINVALID_RESPONSE);

        let call_back = simple_map::borrow(&request_store.request_datas, &request_id).callback_address;
        assert!(ua_address == call_back, EINVALID_RESPONSE);

        //let data_hash = hash::sha3_256(*string::bytes(&data));
        //let res_hash = hash::sha3_256(*string::bytes(simple_map::borrow(&store.response_datas, &request_id)));

        assert!(data == *simple_map::borrow(&response_store.response_datas, &request_id), EINVALID_RESPONSE);

        simple_map::remove(&mut request_store.request_datas, &request_id);
        simple_map::remove(&mut response_store.response_datas, &request_id);

        event::emit(ReceiveResponse { call_back, request_id })
    }

    public entry fun transfer_ownership(owner: &signer, new_owner: address) acquires Roles {
        assert_is_owner(owner);
        let roles = borrow_global_mut<Roles>(oracle_address());
        roles.owner = new_owner
    }

    public entry fun set_operator(owner: &signer, operator: address, authorized: bool) acquires Roles {
        assert_is_owner(owner);
        set_operator_internal(operator, authorized);

        event::emit(OperatorChanged { operator, authorized })
    }

    public entry fun set_operator_internal(operator: address, authorized: bool) acquires Roles {
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

    public entry fun set_fee_account(owner: &signer, fee_account: address) acquires Roles {
        assert_is_owner(owner);
        set_fee_account_internal(fee_account);

        event::emit(FeeAccountChanged{fee_account});
    }

    fun set_fee_account_internal(fee_account: address) acquires Roles {
        let roles = borrow_global_mut<Roles>(oracle_address());
        roles.fee_account = fee_account;

    }

    public entry fun set_messaging_fees(owner: &signer, job_ids: vector<vector<u8>>, amounts: vector<u64>) acquires Roles, OracleStore {
        assert_is_owner(owner);
        let l = vector::length(&job_ids);
        assert!(l == vector::length(&amounts), ELENGHT_MISMATCH);

        let store = borrow_global_mut<OracleStore>(oracle_address());
        vector::enumerate_ref(
            &job_ids,
            |i, job_id| {
                let amount = vector::borrow(&amounts, i);
                simple_map::upsert(&mut store.call_fee, *job_id, *amount);
            }
        );

        event::emit(MessagingFeesChanged{is_delete:false,job_ids,amounts});
    }

    public entry fun del_messaging_fees(owner: &signer, job_ids: vector<vector<u8>>) acquires Roles, OracleStore {
        assert_is_owner(owner);

        let store = borrow_global_mut<OracleStore>(oracle_address());
        vector::for_each(
            job_ids,
            |job_id| {
                simple_map::remove(&mut store.call_fee, &job_id);
            }
        );

        event::emit(MessagingFeesChanged{is_delete:true,job_ids,amounts:vector::empty<u64>()});
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

    fun assert_is_operator(operator: &signer) acquires Roles {
        let roles = borrow_global<Roles>(oracle_address());
        let operator_addr = signer::address_of(operator);
        assert!(smart_vector::contains(&roles.operation_accounts, &operator_addr), EUNAUTHORIZED);
    }

    fun assert_type_signer<TYPE>(account: &signer) {
        assert!(type_address<TYPE>() == signer::address_of(account), EPERMISSION_DENIED);
    }

    #[test_only]
    public fun init_operator_for_test(operator: address) acquires Roles {
        set_operator_internal(operator, true);
    }

    #[test_only]
    public fun init_fee_for_test(fee_account: &signer) acquires Roles {

        let fee_account_address = signer::address_of(fee_account);
        account::create_account_for_test(fee_account_address);
        coin::register<AptosCoin>(fee_account);
        set_fee_account_internal(fee_account_address);
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
}
