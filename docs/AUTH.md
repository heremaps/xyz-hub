# Authorization

Naksha authorization is based upon the Wikvaya User Permission Management. This system generates for every user a user-rights-matrix (URM) and embed this into a [JWT token](https://datatracker.ietf.org/doc/html/rfc7519). Naksha only accepts [JWT token](https://datatracker.ietf.org/doc/html/rfc7519) from trusted services like for example Wikvaya. For open source projects own trusted stores need to be used, which may as well just simply hard coded tokens not requiring a service at local, this is as well the best way for local testing. The [JWT token](https://datatracker.ietf.org/doc/html/rfc7519) must be digitally signed by a trusted partner. The public key of this trusted partner must be added into the Naksha configuration so that the service accepts the tokens.

Once a request is incoming, Naksha will extract the following claims from the [JWT token](https://datatracker.ietf.org/doc/html/rfc7519):

- **aid**: The application identifier (**appId**) and mandatory.
- **uid**: The user identifier (**author**) and optional.
- **urm**: The user access-rights-matrix, it is optional. If not provided, Naksha is using the default matrix from the configuration.
- **su**: Superuser flag, if set, the user will by-pass all security checks. This is implemented by not adding the authorization handler to the event-pipelines.

## Access-Matrix Parameters

## Access-Matrix Verbs

Generally the following verbs are supported:

- **useXXX**: Means that a resource can be used and **id**, **title**, **description** can be viewed.
- **manageXXX**: Means that a resource can be read and modified.
- **createXXX**: Allow the creation of a feature. 
- **readXXX**: Allow reading of a feature.
- **updateXXX**: Allow to update a feature.
- **deleteXXX**: Allow to delete a feature.
- **purgeXXX**: Allow to purge a feature, this is final deletion.

## Access-Matrix Types

- **EventHandler**: 

## Authorization Handler

The authorization handler will generate a request-access-matrix for the event passing through the pipeline and compare the user-access-matrix against it. If the user does have the necessary rights, it will pass the request.

The handler additionally add some result-filtering for the cases, where the user does not have the **manage** rights, but does have the **use** rights.

TODO: addTag + removeTag ?
