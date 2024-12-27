#![allow(unused_attributes)]
#![allow(unused_imports)]
#![allow(unused_results)]
#![allow(unused_variables)]
#![allow(dead_code)]
use kurrent::{
    Client, PersistentSubscriptionEvent, PersistentSubscriptionOptions,
    PersistentSubscriptionToAllOptions, ReplayParkedMessagesOptions, SubscriptionFilter,
};

async fn create_persistent_subscription(client: &Client) -> kurrent::Result<()> {
    // #region create-persistent-subscription-to-stream
    client
        .create_persistent_subscription("test-stream", "subscription-group", &Default::default())
        .await?;
    // #endregion create-persistent-subscription-to-stream

    Ok(())
}

async fn connect_to_persistent_subscription_to_stream(client: &Client) -> kurrent::Result<()> {
    // #region subscribe-to-persistent-subscription-to-stream
    let mut sub = client
        .subscribe_to_persistent_subscription(
            "test-stream",
            "subscription-group",
            &Default::default(),
        )
        .await?;

    loop {
        let event = sub.next().await?;
        // Doing some productive work with the event...
        sub.ack(event).await?;
    }
    // #endregion subscribe-to-persistent-subscription-to-stream
}

async fn connect_to_persistent_subscription_to_all(client: &Client) -> kurrent::Result<()> {
    // #region subscribe-to-persistent-subscription-to-all
    let mut sub = client
        .subscribe_to_persistent_subscription_to_all("subscription-group", &Default::default())
        .await?;

    loop {
        let event = sub.next().await?;
        // Doing some productive work with the event...
        sub.ack(event).await?;
    }
    // #endregion subscribe-to-persistent-subscription-to-all
}

async fn create_persistent_subscription_to_all(client: &Client) -> kurrent::Result<()> {
    // #region create-persistent-subscription-to-all
    let options = PersistentSubscriptionToAllOptions::default()
        .filter(SubscriptionFilter::on_stream_name().add_prefix("test"));

    client
        .create_persistent_subscription_to_all("subscription-group", &options)
        .await?;
    // #endregion create-persistent-subscription-to-all
    Ok(())
}

async fn connect_to_persistent_subscription_with_manual_acks(
    client: &Client,
) -> kurrent::Result<()> {
    // #region subscribe-to-persistent-subscription-with-manual-acks
    let mut sub = client
        .subscribe_to_persistent_subscription(
            "test-stream",
            "subscription-group",
            &Default::default(),
        )
        .await?;

    loop {
        let event = sub.next().await?;
        // Doing some productive work with the event...
        sub.ack(event).await?;
    }
    // #endregion subscribe-to-persistent-subscription-with-manual-acks
}

async fn update_persistent_subscription(client: &Client) -> kurrent::Result<()> {
    // #region update-persistent-subscription
    let options = PersistentSubscriptionOptions::default()
        .resolve_link_tos(true)
        .checkpoint_lower_bound(20);

    client
        .update_persistent_subscription("test-stream", "subscription-group", &options)
        .await?;
    // #endregion update-persistent-subscription

    Ok(())
}

async fn delete_persistent_subscription(client: &Client) -> kurrent::Result<()> {
    // #region delete-persistent-subscription
    client
        .delete_persistent_subscription("test-stream", "subscription-group", &Default::default())
        .await?;
    // #endregion delete-persistent-subscription

    Ok(())
}

async fn get_persistent_subscription_to_stream_info(client: &Client) -> kurrent::Result<()> {
    // #region get-persistent-subscription-to-stream-info
    let info = client
        .get_persistent_subscription_info("test-stream", "subscription-group", &Default::default())
        .await?;

    println!(
        "GroupName: {}, EventStreamId: {}, Status: {:?}",
        info.group_name, info.event_source, info.status
    );
    // #endregion get-persistent-subscription-to-stream-info
    Ok(())
}

async fn get_persistent_subscription_to_all_info(client: &Client) -> kurrent::Result<()> {
    // #region get-persistent-subscription-to-all-info
    let info = client
        .get_persistent_subscription_info_to_all("subscription-group", &Default::default())
        .await?;

    println!(
        "GroupName: {}, EventStreamId: {}, Status: {:?}",
        info.group_name, info.event_source, info.status
    );
    // #endregion get-persistent-subscription-to-all-info
    Ok(())
}

async fn replay_parked_to_stream(client: &Client) -> kurrent::Result<()> {
    // #region replay-parked-of-persistent-subscription-to-stream
    let options = ReplayParkedMessagesOptions::default().stop_at(10);
    client
        .replay_parked_messages("test-stream", "subscription-group", &options)
        .await?;
    // #endregion replay-parked-of-persistent-subscription-to-stream
    Ok(())
}

async fn replay_parked_to_all(client: &Client) -> kurrent::Result<()> {
    // #region replay-parked-of-persistent-subscription-to-all
    let options = ReplayParkedMessagesOptions::default().stop_at(10);
    client
        .replay_parked_messages_to_all("subscription-group", &options)
        .await?;
    // #endregion replay-parked-of-persistent-subscription-to-all
    Ok(())
}

async fn list_persistent_subscription_to_stream(client: &Client) -> kurrent::Result<()> {
    // #region list-persistent-subscriptions-to-stream
    let subscriptions = client
        .list_persistent_subscriptions_for_stream("test-stream", &Default::default())
        .await?;

    for s in subscriptions {
        println!(
            "GroupName: {}, EventStreamId: {}, Status: {:?}",
            s.group_name, s.event_source, s.status
        );
    }
    // #endregion list-persistent-subscriptions-to-stream
    Ok(())
}

async fn list_persistent_subscription_to_all(client: &Client) -> kurrent::Result<()> {
    // #region list-persistent-subscriptions-to-all
    let subscriptions = client
        .list_persistent_subscriptions_to_all(&Default::default())
        .await?;

    for s in subscriptions {
        println!(
            "GroupName: {}, EventStreamId: {}, Status: {:?}",
            s.group_name, s.event_source, s.status
        );
    }
    // #endregion list-persistent-subscriptions-to-all
    Ok(())
}

async fn list_all_persistent_subscription(client: &Client) -> kurrent::Result<()> {
    // #region list-persistent-subscriptions
    let subscriptions = client
        .list_all_persistent_subscriptions(&Default::default())
        .await?;

    for s in subscriptions {
        println!(
            "GroupName: {}, EventStreamId: {}, Status: {:?}",
            s.group_name, s.event_source, s.status
        );
    }
    // #endregion list-persistent-subscriptions
    Ok(())
}

async fn restart_persistent_subscription_subsystem(client: &Client) -> kurrent::Result<()> {
    // #region restart-persistent-subscription-subsystem
    client
        .restart_persistent_subscription_subsystem(&Default::default())
        .await?;
    // #endregion restart-persistent-subscription-subsystem
    Ok(())
}
