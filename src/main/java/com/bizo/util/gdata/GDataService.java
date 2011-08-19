package com.bizo.util.gdata;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.gdata.client.authn.oauth.*;
import com.google.gdata.client.authn.oauth.OAuthParameters.OAuthType;
import com.google.gdata.client.docs.DocsService;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.PlainTextConstruct;
import com.google.gdata.data.docs.DocumentListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.util.ServiceException;

/** Utility class for simplifying talking to the GData API. */
public class GDataService {
  private final String user;
  private final SpreadsheetService spreadsheetService;
  private final DocsService docsService;
  private final OAuthSigner signer = new OAuthHmacSha1Signer();

  public GDataService(final GDataCredentials credentials) {
    Preconditions.checkNotNull(credentials);
    this.user = credentials.getUser();
    final String consumerKey = credentials.getConsumerKey();
    final String consumerSecret = credentials.getConsumerSecret();

    final OAuthParameters ssOAuthParams = new OAuthParameters();
    ssOAuthParams.setOAuthConsumerKey(consumerKey);
    ssOAuthParams.setOAuthConsumerSecret(consumerSecret);
    ssOAuthParams.setOAuthType(OAuthType.TWO_LEGGED_OAUTH);

    final OAuthParameters docOAuthParams = new OAuthParameters();
    docOAuthParams.setOAuthConsumerKey(consumerKey);
    docOAuthParams.setOAuthConsumerSecret(consumerSecret);
    docOAuthParams.setOAuthType(OAuthType.TWO_LEGGED_OAUTH);

    spreadsheetService = new SpreadsheetService("");
    docsService = new DocsService("");

    try {
      spreadsheetService.setOAuthCredentials(ssOAuthParams, signer);
      docsService.setOAuthCredentials(docOAuthParams, signer);
    } catch (final OAuthException e) {
      throw new RuntimeException(e);
    }
  }

  public void createSpreadsheet(final String spreadsheetName) {
    final DocumentListEntry newEntry = new com.google.gdata.data.docs.SpreadsheetEntry();
    newEntry.setTitle(new PlainTextConstruct(spreadsheetName));
    try {
      docsService.insert(new URL("https://docs.google.com/feeds/default/private/full?xoauth_requestor_id=" + user), newEntry);
    } catch (final MalformedURLException e) {
      // should not happen
      throw new RuntimeException(e);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    } catch (final ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public SpreadsheetService getSpreadsheetService() {
    return spreadsheetService;
  }

  public SpreadsheetEntry getSpreadsheet(final String spreadsheetName) {
    try {
      final URL metafeedUrl =
        new URL("https://spreadsheets.google.com/feeds/spreadsheets/private/full?xoauth_requestor_id=" + user);
      final SpreadsheetFeed spreadsheetFeed = spreadsheetService.getFeed(metafeedUrl, SpreadsheetFeed.class);
      final List<SpreadsheetEntry> spreadsheets = spreadsheetFeed.getEntries();

      for (final SpreadsheetEntry spreadsheet : spreadsheets) {
        if (spreadsheetName.equals(spreadsheet.getTitle().getPlainText())) {
          return spreadsheet;
        }
      }

      return null;
    } catch (final MalformedURLException e) {
      // should not happen
      throw new RuntimeException(e);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    } catch (final ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public URL getWorksheetURL(final String spreadsheetName, final String worksheetName) {
    try {
      URL worksheetUrl = null;

      final SpreadsheetEntry spreadsheet = getSpreadsheet(spreadsheetName);
      if (spreadsheet == null) {
        return null;
      }

      final List<WorksheetEntry> worksheets = spreadsheet.getWorksheets();
      for (final WorksheetEntry worksheet : worksheets) {
        if (worksheetName.equals(worksheet.getTitle().getPlainText())) {
          worksheetUrl = worksheet.getListFeedUrl();
          break;
        }
      }

      return worksheetUrl;
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    } catch (final ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public ListFeed getWorksheet(final String spreadsheetName, final String worksheetName) {
    try {
      final URL worksheetURL = getWorksheetURL(spreadsheetName, worksheetName);
      if (worksheetURL == null) {
        return null;
      }
      return spreadsheetService.getFeed(worksheetURL, ListFeed.class);
    } catch (final ServiceException se) {
      throw new RuntimeException(se);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
