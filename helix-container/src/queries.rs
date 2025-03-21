use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::{
    helix_engine::graph_core::traversal::TraversalBuilder,
    helix_engine::graph_core::traversal_steps::{
        SourceTraversalSteps, TraversalBuilderMethods, TraversalMethods, TraversalSearchMethods,
        TraversalSteps,
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::remapping::ResponseRemapping,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::{
        filterable::Filterable, remapping::Remapping, return_values::ReturnValue, value::Value,
    },
};
use sonic_rs::{Deserialize, Serialize};

// Node Schema: User
#[derive(Serialize, Deserialize)]
struct User {
    type_: String,
    display_name: String,
    email: String,
    phone: String,
    status: String,
    profile_image: String,
    description: String,
    rating: i32,
    created_at: String,
    last_modified: String,
}

// Node Schema: Venue
#[derive(Serialize, Deserialize)]
struct Venue {
    venue_name: String,
    description: String,
    address: String,
    size_s_q_m: i32,
    price: f64,
    time_unit: String,
    currency: String,
    max_capacitiy: i32,
    contact_name: String,
    contact_email: String,
    contact_phone: String,
    company_name: String,
    company_website: String,
    company_adress: String,
    status: String,
    created_at: String,
    last_modified: String,
}

// Node Schema: Facility
#[derive(Serialize, Deserialize)]
struct Facility {
    display_name: String,
}

// Node Schema: VenueBooking
#[derive(Serialize, Deserialize)]
struct VenueBooking {
    status: String,
    start_date_time: String,
    end_date_time: String,
    total_cost: f64,
    created_at: String,
    last_modified: String,
}

// Node Schema: Event
#[derive(Serialize, Deserialize)]
struct Event {
    event_name: String,
    description: String,
    start_date_time: String,
    end_date_time: String,
    status: String,
    price: f64,
    currency: String,
    max_capacitiy: i32,
    rating: i32,
    created_at: String,
    last_modified: String,
}

// Edge Schema: SavedVenue
#[derive(Serialize, Deserialize)]
struct SavedVenue {}

// Edge Schema: RatesVenue
#[derive(Serialize, Deserialize)]
struct RatesVenue {
    rating: i32,
}

// Edge Schema: BookedVenue
#[derive(Serialize, Deserialize)]
struct BookedVenue {}

// Edge Schema: HasFacility
#[derive(Serialize, Deserialize)]
struct HasFacility {}

// Edge Schema: Booking
#[derive(Serialize, Deserialize)]
struct Booking {}

// Edge Schema: HeldAt
#[derive(Serialize, Deserialize)]
struct HeldAt {}

// Edge Schema: CreatedEvent
#[derive(Serialize, Deserialize)]
struct CreatedEvent {}

// Edge Schema: SavedEvent
#[derive(Serialize, Deserialize)]
struct SavedEvent {}

// Edge Schema: BookedEvent
#[derive(Serialize, Deserialize)]
struct BookedEvent {
    quantity: i32,
    cost_per_unit: i32,
    status: String,
    created_at: String,
    last_modified: String,
}

// Edge Schema: RatedEvent
#[derive(Serialize, Deserialize)]
struct RatedEvent {
    rating: i32,
}

// Edge Schema: CreatedVenue
#[derive(Serialize, Deserialize)]
struct CreatedVenue {}

#[handler]
pub fn get_created_events(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct GetCreatedEventsData {
        user_i_d_p: String,
        limit: i32,
        last_page: i32,
    }

    let data: GetCreatedEventsData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    tr.out(&txn, "CreatedEvent");
    tr.range(data.limit, data.last_page);
    let events = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn get_bookings_for_event(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct GetBookingsForEventData {
        event_i_d_p: String,
        limit: i32,
        last_page: i32,
    }

    let data: GetBookingsForEventData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.event_i_d_p);
    tr.in_e(&txn, "BookedEvent");
    tr.range(data.limit, data.last_page);
    let bookings = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn get_user_saved_events(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct GetUserSavedEventsData {
        user_i_d_p: String,
        limit: i32,
        last_page: i32,
    }

    let data: GetUserSavedEventsData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    tr.out(&txn, "SavedEvent");
    tr.range(data.limit, data.last_page);
    let events = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn update_venue_booking(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct UpdateVenueBookingData {
        venue_booking_i_d_p: String,
        start_date_time_p: String,
        end_date_time_p: String,
        total_cost_p: f64,
        status_p: String,
    }

    let data: UpdateVenueBookingData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.venue_booking_i_d_p);
    let venueBooking = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.current_step = TraversalValue::from(venueBooking.clone());
    tr.out(&txn, "Booking");
    tr.for_each_node(&txn, |item, txn| {
        let id_remapping = Remapping::new(false, None, None);
        remapping_vals.borrow_mut().insert(
            item.id.clone(),
            ResponseRemapping::new(HashMap::from([("id".to_string(), id_remapping)]), false),
        );
        Ok(())
    });
    let venueID = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.current_step = TraversalValue::from(venueBooking.clone());
    tr.update_props(&mut txn, props!{ "startDateTime".to_string() => data.start_date_time_p, "endDateTime".to_string() => data.end_date_time_p, "totalCost".to_string() => data.total_cost_p, "status".to_string() => data.status_p });
    let v = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn unsave_venue(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct UnsaveVenueData {
        venue_i_d_p: String,
        user_i_d_p: String,
    }

    let data: UnsaveVenueData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    tr.out_e(&txn, "SavedVenue");
    tr.filter_nodes(&txn, |node| {
        Ok({
            let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));
            tr.in_v(&txn);
            node.check_property("id").map_or(
                false,
                |v| matches!(v, Value::String(val) if *val == "venueID_p"),
            )
        })
    });
    tr.drop(&mut txn);
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn delete_venue(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct DeleteVenueData {
        venue_i_d_p: String,
    }

    let data: DeleteVenueData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.venue_i_d_p);
    let venue = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.current_step = TraversalValue::from(venue.clone());
    tr.out_e(&txn, "");
    tr.drop(&mut txn);
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.current_step = TraversalValue::from(venue.clone());
    tr.in_e(&txn, "");
    tr.drop(&mut txn);
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.drop(&mut txn);
    tr.drop(&mut txn);
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn save_event(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct SaveEventData {
        user_i_d_p: String,
        event_i_d_p: String,
    }

    let data: SaveEventData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    let user = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.event_i_d_p);
    let event = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(
        &mut txn,
        "SavedEvent",
        &user.get_id()?,
        &event.get_id()?,
        props! {},
    );
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn unsave_event(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct UnsaveEventData {
        user_i_d_p: String,
        event_i_d_p: String,
    }

    let data: UnsaveEventData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    tr.out_e(&txn, "SavedEvent");
    tr.filter_nodes(&txn, |node| {
        Ok({
            let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));
            tr.in_v(&txn);
            node.check_property("id").map_or(
                false,
                |v| matches!(v, Value::String(val) if *val == "eventID_p"),
            )
        })
    });
    tr.drop(&mut txn);
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn create_event_booking(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct CreateEventBookingData {
        user_i_d_p: String,
        event_i_d_p: String,
        quantity_p: i32,
        cost_per_unit_p: f64,
        status_p: String,
        created_at_p: String,
        last_modified_p: String,
    }

    let data: CreateEventBookingData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    let user = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.event_i_d_p);
    let event = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(&mut txn, "BookedEvent", &user.get_id()?, &event.get_id()?, props!{ "quantity".to_string() => "quantity_p", "costPerUnit".to_string() => "costPerUnit_p", "status".to_string() => "status_p", "createdAt".to_string() => "createdAt_p", "lastModified".to_string() => "lastModified_p" });
    let booking = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.current_step = TraversalValue::from(booking.clone());
    tr.for_each_node(&txn, |item, txn| {
        let id_remapping = Remapping::new(false, None, None);
        remapping_vals.borrow_mut().insert(
            item.id.clone(),
            ResponseRemapping::new(HashMap::from([("id".to_string(), id_remapping)]), false),
        );
        Ok(())
    });
    let id = tr.finish()?;

    txn.commit()?;
    return_vals.insert(
        "id".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(id, remapping_vals.borrow_mut()),
    );
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn get_user_event_bookings(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct GetUserEventBookingsData {
        user_i_d_p: String,
        limit: i32,
        last_page: i32,
    }

    let data: GetUserEventBookingsData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    tr.out_e(&txn, "BookedEvent");
    tr.range(data.limit, data.last_page);
    let eventBookings = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn save_venue(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct SaveVenueData {
        venue_i_d_p: String,
        user_i_d_p: String,
    }

    let data: SaveVenueData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    let user = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.venue_i_d_p);
    let venue = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(
        &mut txn,
        "SavedVenue",
        &user.get_id()?,
        &venue.get_id()?,
        props! {},
    );
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn create_venue_booking(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct CreateVenueBookingData {
        venue_i_d_p: String,
        user_i_d_p: String,
        event_i_d_p: String,
        start_date_time_p: i32,
        end_date_time_p: i32,
        total_cost_p: f64,
        status_p: String,
        created_at_p: String,
        last_modified_p: String,
    }

    let data: CreateVenueBookingData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(&mut txn, "VenueBooking", props!{ "startDateTime".to_string() => "startDateTime_p", "endDateTime".to_string() => "endDateTime_p", "totalCost".to_string() => "totalCost_p", "status".to_string() => "status_p", "createdAt".to_string() => "createdAt_p", "lastModified".to_string() => "lastModified_p" }, None);
    let venueBooking = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.venue_i_d_p);
    let venue_V = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    let uesr = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.event_i_d_p);
    let event = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(
        &mut txn,
        "Booking",
        &venueBooking.get_id()?,
        "venue",
        props! {},
    );
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(
        &mut txn,
        "BookedVenue",
        "user",
        &venueBooking.get_id()?,
        props! {},
    );
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(
        &mut txn,
        "HeldAt",
        &event.get_id()?,
        &venueBooking.get_id()?,
        props! {},
    );
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn cancel_event_booking(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct CancelEventBookingData {
        event_booking_i_d_p: String,
    }

    let data: CancelEventBookingData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.e_from_id(&txn, &data.event_booking_i_d_p);
    tr.update_props(&mut txn, props! { "status".to_string() => "cancelled" });
    let edge = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn get_user_saved_venues(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct GetUserSavedVenuesData {
        user_i_d_p: String,
        limit: i32,
        last_page: i32,
    }

    let data: GetUserSavedVenuesData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    tr.out(&txn, "SavedVenue");
    tr.range(data.limit, data.last_page);
    let venues = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn rate_venue(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct RateVenueData {
        venue_i_d_p: String,
        user_i_d_p: String,
        rating_p: i32,
    }

    let data: RateVenueData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    let user = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.venue_i_d_p);
    let venue = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(
        &mut txn,
        "RatesVenue",
        &user.get_id()?,
        &venue.get_id()?,
        props! { "rating".to_string() => "rating_p" },
    );
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn cancel_venue_booking(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct CancelVenueBookingData {
        venue_booking_i_d_p: String,
    }

    let data: CancelVenueBookingData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.venue_booking_i_d_p);
    let venueBooking = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.current_step = TraversalValue::from(venueBooking.clone());
    tr.update_props(&mut txn, props! { "status".to_string() => "cancelled" });
    let l = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.current_step = TraversalValue::from(venueBooking.clone());
    tr.in_e(&txn, "HeldAt");
    tr.drop(&mut txn);
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn get_all_venues(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct GetAllVenuesData {
        limit: i32,
        last_page: i32,
    }

    let data: GetAllVenuesData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["Venue"]);
    tr.filter_nodes(&txn, |node| {
        Ok(node.check_property("status").map_or(
            false,
            |v| matches!(v, Value::String(val) if *val == "active"),
        ))
    });
    tr.range(data.limit, data.last_page);
    let venues = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn get_all_venues_and_bookings(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct GetAllVenuesAndBookingsData {
        limit_p: i32,
        last_page_p: i32,
    }

    let data: GetAllVenuesAndBookingsData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(2);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["Venue"]);
    tr.range(data.limit_p, data.last_page_p);
    let venuesData = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["VenueBooking"]);
    tr.range(data.limit_p, data.last_page_p);
    let venueBookingsData = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn update_venue(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct UpdateVenueData {
        venue_i_d_p: String,
        venue_name_p: String,
        description_p: String,
        address_p: String,
        size_s_q_m_p: i32,
        price_p: i32,
        currency_p: String,
        time_unit_p: String,
        max_capacitiy_p: i32,
        facilities_p: Vec<Facility>,
        start_date_time_p: String,
        end_date_time_p: String,
        contact_name_p: String,
        contact_email_p: String,
        contact_phone_p: String,
        status_p: String,
        company_name_p: String,
        company_website_p: String,
        company_adress_p: String,
        status_p: String,
        created_at_p: String,
    }

    let data: UpdateVenueData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.venue_i_d_p);
    let venue = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.current_step = TraversalValue::from(venue.clone());
    tr.update_props(&mut txn, props!{ "venueName".to_string() => data.venue_name_p, "description".to_string() => data.description_p, "address".to_string() => data.address_p, "sizeSQM".to_string() => data.size_s_q_m_p, "price".to_string() => data.price_p, "currency".to_string() => data.currency_p, "timeUnit".to_string() => data.time_unit_p, "maxCapacity".to_string() => data.max_capacitiy_p, "startDateTime".to_string() => data.start_date_time_p, "endDateTime".to_string() => data.end_date_time_p, "contactName".to_string() => data.contact_name_p, "contactEmail".to_string() => data.contact_email_p, "contactPhone".to_string() => data.contact_phone_p, "companyName".to_string() => data.company_name_p, "companyWebsite".to_string() => data.company_website_p, "companyAdress".to_string() => data.company_adress_p, "status".to_string() => data.status_p });
    let v = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.current_step = TraversalValue::from(venue.clone());
    tr.out_e(&txn, "HasFacility");
    tr.drop(&mut txn);
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn rate_event(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct RateEventData {
        user_i_d_p: String,
        event_i_d_p: String,
        rating_p: i32,
    }

    let data: RateEventData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    let user = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.event_i_d_p);
    let event = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(
        &mut txn,
        "RatedEvent",
        &user.get_id()?,
        &event.get_id()?,
        props! { "rating".to_string() => "rating_p" },
    );
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn create_event(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct CreateEventData {
        user_i_d_p: String,
        event_name_p: String,
        description_p: String,
        start_date_time_p: i32,
        end_date_time_p: i32,
        status_p: String,
        price_p: f64,
        currency_p: String,
        max_capacity_p: i32,
        rating_p: i32,
        created_at_p: String,
        last_modified_p: String,
    }

    let data: CreateEventData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.user_i_d_p);
    let user = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(&mut txn, "Event", props!{ "eventName".to_string() => "eventName_p", "description".to_string() => "description_p", "startDateTime".to_string() => "startDateTime_p", "endDateTime".to_string() => "endDateTime_p", "status".to_string() => "status_p", "price".to_string() => "price_p", "currency".to_string() => "currency_p", "maxCapacity".to_string() => "maxCapacity_p", "rating".to_string() => "rating_p", "createdAt".to_string() => "createdAt_p", "lastModified".to_string() => "lastModified_p" }, None);
    let event = tr.finish()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(
        &mut txn,
        "CreatedEvent",
        &user.get_id()?,
        &event.get_id()?,
        props! {},
    );
    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn get_all_events(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct GetAllEventsData {
        user_i_d_p: String,
        limit: i32,
        last_page: i32,
    }

    let data: GetAllEventsData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["Event"]);
    tr.filter_nodes(&txn, |node| {
        Ok(node.check_property("status").map_or(
            false,
            |v| matches!(v, Value::String(val) if *val == "active"),
        ) && {
            let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));
            tr.in_(&txn, "BookedEvent");
            node.check_property("id").map_or(
                false,
                |v| matches!(v, Value::String(val) if *val != "userID_p"),
            )
        })
    });
    tr.range(data.limit, data.last_page);
    let events = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn cancel_event(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct CancelEventData {
        event_i_d_p: String,
    }

    let data: CancelEventData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, &data.event_i_d_p);
    tr.update_props(&mut txn, props! { "status".to_string() => "cancelled" });
    let event = tr.finish()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}
