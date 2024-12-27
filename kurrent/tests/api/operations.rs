use kurrent::operations;
use kurrent::operations::StatsOptions;
use std::time::Duration;

fn generate_login(names: &mut names::Generator<'_>) -> String {
    names.next().unwrap().replace("-", "_")
}

async fn test_gossip(client: &operations::Client) -> kurrent::Result<()> {
    let gossip = client.read_gossip().await?;

    assert!(gossip.len() > 0);

    Ok(())
}

async fn test_stats(client: &operations::Client) -> kurrent::Result<()> {
    let options = StatsOptions::default().refresh_time(Duration::from_millis(500));

    let mut stream = client.stats(&options).await?;
    let result = stream.next().await?;

    assert!(!result.0.is_empty());
    Ok(())
}

async fn test_create_user(
    client: &operations::Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    client
        .create_user(
            generate_login(names),
            names.next().unwrap(),
            names.next().unwrap(),
            Vec::new(),
            &Default::default(),
        )
        .await?;

    Ok(())
}

async fn test_update_user(
    client: &operations::Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let login = generate_login(names);

    client
        .create_user(
            login.as_str(),
            names.next().unwrap(),
            names.next().unwrap(),
            Vec::new(),
            &Default::default(),
        )
        .await?;

    client
        .update_user(
            login.as_str(),
            names.next().unwrap(),
            names.next().unwrap(),
            Vec::new(),
            &Default::default(),
        )
        .await?;

    Ok(())
}

async fn test_delete_user(
    client: &operations::Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let login = generate_login(names);

    client
        .create_user(
            login.as_str(),
            names.next().unwrap(),
            names.next().unwrap(),
            Vec::new(),
            &Default::default(),
        )
        .await?;

    client
        .delete_user(login.as_str(), &Default::default())
        .await?;

    Ok(())
}

async fn test_enable_user(
    client: &operations::Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let login = generate_login(names);

    client
        .create_user(
            login.as_str(),
            names.next().unwrap(),
            names.next().unwrap(),
            Vec::new(),
            &Default::default(),
        )
        .await?;

    client
        .enable_user(login.as_str(), &Default::default())
        .await?;

    Ok(())
}

async fn test_disable_user(
    client: &operations::Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let login = generate_login(names);

    client
        .create_user(
            login.as_str(),
            names.next().unwrap(),
            names.next().unwrap(),
            Vec::new(),
            &Default::default(),
        )
        .await?;

    client
        .enable_user(login.as_str(), &Default::default())
        .await?;

    client
        .disable_user(login.as_str(), &Default::default())
        .await?;

    Ok(())
}

async fn test_user_details(
    client: &operations::Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let login = generate_login(names);

    client
        .create_user(
            login.as_str(),
            names.next().unwrap(),
            names.next().unwrap(),
            Vec::new(),
            &Default::default(),
        )
        .await?;

    let result = client
        .user_details(login.as_str(), &Default::default())
        .await;

    assert!(result.is_ok());

    Ok(())
}

async fn test_change_user_password(
    client: &operations::Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let login = generate_login(names);
    let password = names.next().unwrap();

    client
        .create_user(
            login.as_str(),
            password.as_str(),
            names.next().unwrap(),
            Vec::new(),
            &Default::default(),
        )
        .await?;

    client
        .change_user_password(
            login.as_str(),
            password,
            names.next().unwrap(),
            &Default::default(),
        )
        .await?;

    Ok(())
}

async fn test_reset_user_password(
    client: &operations::Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let login = generate_login(names);

    client
        .create_user(
            login.as_str(),
            names.next().unwrap(),
            names.next().unwrap(),
            Vec::new(),
            &Default::default(),
        )
        .await?;

    client
        .reset_user_password(login.as_str(), names.next().unwrap(), &Default::default())
        .await?;

    Ok(())
}

async fn test_merge_indexes(client: &operations::Client) -> kurrent::Result<()> {
    client.merge_indexes(&Default::default()).await
}

async fn test_resign_node(client: &operations::Client) -> kurrent::Result<()> {
    client.resign_node(&Default::default()).await
}

async fn test_set_node_priority(client: &operations::Client) -> kurrent::Result<()> {
    client.set_node_priority(1, &Default::default()).await
}

async fn test_op_restart_persistent_subscription_subsystem(
    client: &operations::Client,
) -> kurrent::Result<()> {
    client
        .restart_persistent_subscriptions(&Default::default())
        .await
}

async fn test_scavenge(client: &operations::Client) -> kurrent::Result<()> {
    let result = client.start_scavenge(1, 0, &Default::default()).await?;
    let result = client.stop_scavenge(result.id(), &Default::default()).await;

    assert!(result.is_ok());

    Ok(())
}

async fn test_shutdown(client: &operations::Client) -> kurrent::Result<()> {
    client.shutdown(&Default::default()).await
}

pub async fn tests(client: kurrent::Client) -> eyre::Result<()> {
    let mut gen = names::Generator::default();
    let gen = &mut gen;
    let client: operations::Client = client.into();
    let client = &client;

    debug!("Before test_gossip…");
    test_gossip(client).await?;
    debug!("Complete");
    debug!("Before test_stats…");
    if let Err(e) = test_stats(client).await {
        if !e.is_unsupported_feature() {
            Err(e)?;
        }
    }
    debug!("Complete");
    debug!("Before test_create_user…");
    test_create_user(client, gen).await?;
    debug!("Complete");
    debug!("Before test_update_user…");
    test_update_user(client, gen).await?;
    debug!("Complete");
    debug!("Before test_delete_user…");
    test_delete_user(client, gen).await?;
    debug!("Complete");
    debug!("Before test_enable_user…");
    test_enable_user(client, gen).await?;
    debug!("Complete");
    debug!("Before test_disable_user…");
    test_disable_user(client, gen).await?;
    debug!("Complete");
    debug!("Before test_user_details…");
    test_user_details(client, gen).await?;
    debug!("Complete");
    debug!("Before test_change_user_password…");
    test_change_user_password(client, gen).await?;
    debug!("Complete");
    debug!("Before test_reset_user_password…");
    test_reset_user_password(client, gen).await?;
    debug!("Complete");
    debug!("Before test_merge_indexes…");
    test_merge_indexes(client).await?;
    debug!("Complete");
    debug!("Before test_resign_node…");
    test_resign_node(client).await?;
    debug!("Complete");
    debug!("Before test_set_node_priority…");
    test_set_node_priority(client).await?;
    debug!("Complete");
    debug!("Before test_op_restart_persistent_subscription_subsystem…");
    test_op_restart_persistent_subscription_subsystem(client).await?;
    debug!("Complete");
    debug!("Before test_scavenge…");
    test_scavenge(client).await?;
    debug!("Complete");
    debug!("Before test_shutdown…");
    test_shutdown(client).await?;
    debug!("Complete");

    Ok(())
}
