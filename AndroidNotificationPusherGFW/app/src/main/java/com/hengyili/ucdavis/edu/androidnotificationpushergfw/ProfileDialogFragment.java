package com.hengyili.ucdavis.edu.androidnotificationpushergfw;

import static android.content.Context.MODE_PRIVATE;

import android.app.Activity;
import android.app.Dialog;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Bundle;
import android.provider.MediaStore;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.EditText;
import android.widget.ImageView;
import androidx.activity.result.ActivityResultLauncher;
import androidx.annotation.NonNull;
import androidx.appcompat.app.AlertDialog;
import androidx.fragment.app.DialogFragment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ProfileDialogFragment extends DialogFragment
{
  private ActivityResultLauncher<Intent> mGetContent;
  private static final int PICK_IMAGE = 1;
  private ImageView ivAvatar;

  public Uri getImageUri(Context context, Bitmap inImage)
  {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    inImage.compress(Bitmap.CompressFormat.JPEG, 100, bytes);
    String path = MediaStore.Images.Media.insertImage(context.getContentResolver(), inImage, "Title", null);
    return Uri.parse(path);
  }



  @NonNull
  @Override
  public Dialog onCreateDialog(Bundle savedInstanceState)
  {
    AlertDialog.Builder builder = new AlertDialog.Builder(requireActivity());
    LayoutInflater inflater = requireActivity().getLayoutInflater();

    View view = inflater.inflate(R.layout.dialog_profile, null);

    ivAvatar = view.findViewById(R.id.iv_avatar);
    EditText etName = view.findViewById(R.id.et_name);
    EditText etEmail = view.findViewById(R.id.et_email);
    EditText etPhone = view.findViewById(R.id.et_phone);

    // Load the profile information
    String[] profile = ((ProfileActivity) requireActivity()).loadProfile();
    etName.setText(profile[0]);
    etEmail.setText(profile[1]);
    etPhone.setText(profile[2]);

    // Load the avatar from SharedPreferences and set it in the ImageView
    String avatarUri = loadAvatar();
    if (!avatarUri.isEmpty())
    {
      ivAvatar.setImageURI(Uri.parse(avatarUri));
    }

    // Set a click listener on the ImageView to allow the user to select an image from the gallery
    ivAvatar.setOnClickListener(v ->
    {
      Intent intent = new Intent(Intent.ACTION_PICK, MediaStore.Images.Media.EXTERNAL_CONTENT_URI);
      startActivityForResult(intent, PICK_IMAGE);
    });

    builder.setView(view).setPositiveButton("Save", (dialog, id) ->
    {
      // Save the profile information
      String name = etName.getText().toString();
      String email = etEmail.getText().toString();
      String phone = etPhone.getText().toString();
      ((ProfileActivity) requireActivity()).saveProfile(name, email, phone);
      dialog.dismiss();
    }).setNegativeButton("Cancel", (dialog, id) ->
    {
      // User cancelled the dialog
      dialog.dismiss();
    });

    return builder.create();
  }

  @Override
  public void onActivityResult(int requestCode, int resultCode, Intent data)
  {
    super.onActivityResult(requestCode, resultCode, data);

    requireActivity();
    if (requestCode == PICK_IMAGE && resultCode == Activity.RESULT_OK && data != null)
    {
      Uri selectedImage = data.getData();

      try
      {
        // Get the bitmap of the selected image
        Bitmap bitmap = MediaStore.Images.Media.getBitmap(requireActivity().getContentResolver(), selectedImage);

        // Calculate the starting x and y coordinates
        int startx = (bitmap.getWidth() - bitmap.getHeight()) / 2;
        int starty = (bitmap.getHeight() - bitmap.getWidth()) / 2;

        // Make sure startx and starty are not negative
        startx = Math.max(startx, 0);
        starty = Math.max(starty, 0);

        // Calculate the size of the new bitmap
        int size = Math.min(bitmap.getWidth(), bitmap.getHeight());

        // Create a new bitmap that is a square crop of the original bitmap
        Bitmap croppedBitmap = Bitmap.createBitmap(bitmap, startx, starty, size, size);

        // Convert the cropped bitmap back to a Uri
        Uri croppedImageUri = getImageUri(requireActivity(), croppedBitmap);

        // Set the cropped image in the ImageView
        ivAvatar.setImageURI(croppedImageUri);

        // Save the cropped image URI in SharedPreferences
        SharedPreferences sharedPreferences = requireActivity().getSharedPreferences("Profile", MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putString("Avatar", croppedImageUri.toString());
        editor.apply();
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
    }
  }
    public String loadAvatar()
    {
        SharedPreferences sharedPreferences = requireActivity().getSharedPreferences("Profile", MODE_PRIVATE);
        return sharedPreferences.getString("Avatar", "");
    }
}